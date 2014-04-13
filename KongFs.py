#!/usr/bin/env python

## Module: KongFS.py 
## Purpose: A FUSE filesystem driver which uses mongodb as a
## Backend. 
## Matt Arnold for SUNYIT CS350 
## Start-Date: 9/1/2012
## Recovered from Backup on 12/3/12
## Copyright (C) 2012-2014 Matt Arnold
## Licensed under the Academic Free License version 3.0

from stat import *
from errno import *
from time import time
from sys import argv
import os
import types
import logging
import pymongo
from StringIO import StringIO
from pymongo import Connection
from bson import ObjectId  # 10Gen why do you make modules whose
# namespace conflicts with python standard libs, are you rude guys just
# huffing glue, or something
import gridfs
## Note: GridFS is not an actual filesystem. The 10Gen guys were smoking
## something pretty strong, and most likely drunk when they thought of it. It is supposed to be a
## "filesystem for storing user uploaded contet" but it's not a filesystem
## for one thing it can only be accessed using their in house protocal.
## and it only gets worse, they store directories as part of the filename
## Another thing they store wrong attributes the MIME-Type for example
## which should never be stored because it may vary from system to system
## and they don't store enough attributes in some cases i.e no st_mode which 
## is absolutly criticl on posix systems. However they do implement journaled
## Inodes correctly, which removes a large obstacle here so we shall use it,
## and thank the gods for inventing function pointers :)
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn, fuse_get_context
FuseOsError = FuseOSError
KONGFS_VERSION = 1
STATATTR_KEY = 'attribs'


def get_metadata(self):
    
    # portians of this code are taken from MongoDb and Python 
    # O'Higgens 2011, used because he gives the only safe way to accuire a
    # database handle, and if i wrote it  myself it would look like i had
    # copied it so why not just copy it.
    metadata = None
    if type(self) == str:
        
        d, cl, scl = self.split('.')
        try:
            c = Connection(host="localhost", port=27017)
        except ConnectionFailure, e:
            raise RuntimeError('Connection Failure, is mongd running?')
        dbh = c[d]
        assert dbh.connection == c
        ocl = dbh[cl]
        metadata = ocl[scl]
    elif isinstance(self, gridfs.GridFS):
        try:
            metadata = self._files 
        except TypeError, e:
            metadata = self.get_metadata()
    else:
        raise RuntimeError("""For Unmounted filesystem you must pass the
        of metadata collection as first argument""")
    return metadata



        
def cast_ObjectId(self,o):
    
    if isinstance(o, ObjectId):
        return o
    else:
        return ObjectId(o)


def gen_defstat_attrs(self):
    
    now = time() # Python is profound
    context = fuse_get_context()
    return dict(st_mode= S_IFREG | 0644,
    st_ctime=now,st_mtime=now,st_atime=now,st_uid=context[0],st_gid=context[1],
    st_nlink=1)

# Welcome to python where everthing is indirection
def update_fstats(self, oid, attr, value):
    
    files_col = get_metadata(self)
    
    filestruct = files_col.find_one({"_id":cast_ObjectId(self,oid)})

    print filestruct
    if STATATTR_KEY in filestruct:
        setvalue = {STATATTR_KEY + '.0.' + attr: value}
        files_col.update({"_id":filestruct['_id']},{"$set":setvalue})
            
    else:
        #d  = {"_id":filestruct['_id']}, {"$push":{STATATTR_KEY:gen_defstat_attrs()}}
        files_col.update({'_id':filestruct['_id']},{"$push":{STATATTR_KEY:gen_defstat_attrs(self)}})
        setvalue = {STATATTR_KEY + '.0.' + attr: value}
        files_col.update({"_id":filestruct['_id']},{"$set":setvalue})


    #return 0

# Note on these next two methods I'm making no attempt to catch
# exceptions/check for errors, because if the attributes are not where i expect
# them to be when these are called, i.e at position 0 of the STATATTR_KEY
# subdocument we have metadata curroption and at this stage of dev i want
# violent failure if that happen. Error checking will be added in capstone but
# by then most of this  stuff will be written in C so
def remove_attr(self, oid, attr):

    md = get_metadata(self)
    filestruct = md.find_one({"_id":cast_ObjectId(self, oid)})
    del filestruct[STATATTR_KEY][0][attr]
    md.save(filestruct)

# Yet another function to implement getattr

def raw_getattr(self, oid):
    
    md = get_metadata(self)
    filestruct = md.find_one({"_id":cast_ObjectId(self, oid)})
    return filestruct[STATATTR_KEY] # that data better exist by the time we
    # call this or else it's curropted



class KongFs(Operations,LoggingMixIn):

    def __init__(self, dbhost, dbport, database):

        try:
            c = Connection(host=dbhost, port=dbport)
        except ConnectionFailure, e:
            raise RuntimeError("Connect failed")
        self.dbh = c[database]
        assert self.dbh.connection == c
        self.datastore = gridfs.GridFS(self.dbh)

        m1 = types.MethodType(get_metadata, self.datastore)
        m2 = types.MethodType(cast_ObjectId, self.datastore)
        m3 = types.MethodType(gen_defstat_attrs, self.datastore)
        m4 = types.MethodType(update_fstats, self.datastore)
        m5 = types.MethodType(remove_attr, self.datastore)
        m6 = types.MethodType(raw_getattr, self.datastore)
        
        self.datastore.get_metadata = m1
        self.datastore.cast_ObjectId = m2
        self.datastore.gen_attrs = m3
        self.datastore.update_attr = m4
        self.datastore.remove_attr = m5
        self.datastore.raw_getattr = m6
        self.datastore._files = self.dbh.fs.files
        # on second thought i should've subclassed GridFs but
        # to late now
        self.fd = 0
    
    def getattr(self, path, fh=None):
        if path == '/':
	  # Ordinarily we wouldn't emulate / it would have it's own metadata
	  # But as GridFs is somewhat stupid we must
           return dict(st_mode=(S_IFDIR | 0755), st_nlink=2)
        if self.datastore.exists(filename=path[1:]): 
        # [1:] removes leading / bad things happen if you do not
            
            dp = self.datastore.get_last_version(filename=path[1:])
            rawattr = self.datastore.raw_getattr(dp._id)
            rawattr['st_size'] = dp.length
            del dp
            return rawattr
        
        else:
            raise FuseOsError(ENOENT)

    def getxattr(self, path, name, pos=0):

        if self.datastore.exists(filename=path[1:]):
            
            dp = self.datastore.get_last_version(filename=path[1:])
            rawattrs = self.datastore.raw_getattr(dp._id)
            if name in rawattrs:
                return rawattr[name]
            elif name == 'st_size':
                return dp.length
            else:
                return '' # API docs say to do this even though it's wrong
                # should be ENOATTR
        else: # ds.exists
            raise FuseOsError(ENOENT)

    def setxattr(self, path, name, value, opts, pos=0):

        if self.datastore.exists(filename=path[1:]):
            
            dp = self.datastore.get_last_version(filename=path[1:])
            if name != 'st_size':
                self.datstore.update_attr(dp._id, name, value)
            else:
                pass 
                # st_size must always be dp.length
        else: # ds.exists
            raise FuseOsError(ENOENT)

    
    def removexattr(self, path, name):

        if self.datastore.exists(filename=path[1:]):
            dp = self.datastore.get_last_version(filename=path[1:])
            if name != 'st_size':
                try:
                    self.remove_attr(dp._id, name)
                except KeyError:
                    pass
            else: 
                pass
        else:
            raise FuseOsError(ENOENT)

    def listxattr(self, path):
        
        if self.datastore.exists(filename=path[1:]):

            dp = self.datastore.get_last_version(filename=path[1:])
            rawattrs = self.raw_getattr(dp._id)
            xattrs = rawattrs.keys().append('st_size')
            return xattrs
        else:
            raise FuseOsError(ENOENT)

    def open(self, path, flags):

        self.fd += 1
        return self.fd


    def read(self, path, size, offset, fh):
        print "** Read from file: " + path[1:] + "**"
        print "Size: " + str(size)
        print "Offset: " + str(offset)
        data = None
        if self.datastore.exists(filename=path[1:]):
            dp = self.datastore.get_last_version(filename=path[1:])
            try:
                dp.seek(offset)
                data = dp.read(size)
                dp.close()
                return data
            except Exception as e:
                print e
                raise FuseOsError(EIO)
                
        else:
            raise FuseOSError(ENOENT)

    def create(self, path, mode):
        
    
        
        self.datastore.put('', filename=path[1:],
        attribs=self.datastore.gen_attrs())
        dp = self.datastore.get_last_version(filename=path[1:])
        self.datastore.update_attr(dp._id, 'st_mode', mode)
        self.fd += 1
        return self.fd

    

    def write(self, path, data, offset, fh):

        if self.datastore.exists(filename=path[1:]):
            dp = self.datastore.get_last_version(filename=path[1:])
            print "**Write on file: " + path[1:] + "**"
            print "Offset: " + str(offset)
            print "File Length: " + str(dp.length)
            print str(type(data))
            try:
                membuffer = StringIO()
                membuffer.write(dp.read())
                if offset <= dp.length:
                    membuffer.seek(offset)
                
                membuffer.write(data)
                self.datastore.put(membuffer.getvalue(), filename=path[1:],
                    attribs=self.datastore.raw_getattr(dp._id))
                membuffer.close()
                return len(data)
            except Exception as e:
                print e
                raise FuseOsError(EIO)
        
        
        else:
            raise FuseOsError(ENOENT)
    # As GridFiles Can have multiple versions we remove duplicate entries
    def readdir(self, path, fh):

        return ['.','..'] + list(set(self.datastore.list()))

    def chmod(self, path, mode):

        if self.datastore.exists(filename=path[1:]):
            dp = self.datastore.get_last_version(filename=path[1:])
            rawattrs = self.datastore.raw_getattr(dp._id)
            newmode = rawattrs['st_mode']
            newmode &= 0770000
            newmode |= mode
            self.datastore.update_attr(dp._id, 'st_mode', newmode)
            return 0
        else:
            raise FuseOsError(ENOENT)

    def chown(self, path, uid, gid):
    
        if self.datastore.exists(filename=path[1:]):
            dp = self.datastore.get_last_version(filename=path[1:])
            self.datastore.update_attr(dp._id, 'st_uid', uid)
            self.datastore.update_attr(dp._id, 'st_gid', gid)
        else:
            raise FuseOsError(ENOENT)

    def unlink(self, path):
        
        curr = self.dbh.fs.files.find({'filename':path[1:]})
        for rec in curr:
            self.datastore.delete(rec["_id"])

    def rename(self, old, new):
        ga = self.datastore.raw_getattr
        if self.datastore.exists(filename=path[1:]):
            dp = self.datastore.get_last_version(filename=path[1:])
            filedata = dp.read()
            self.datastore.put(filedata, filename=new, attribs=ga(dp._id))
            self.unlink(old)
        else:
            raise FuseOsError(ENOENT)
    def truncate(self, path, length, fh=None):
        ga = self.datastore.raw_getattr
        if self.datastore.exists(filename=path[1:]):
            dp = self.datastore.get_last_version(path[1:])
            if dp.length < length:
                return
            dp.seek(length)
            trucdata = dp.read()
            self.datastore.put(trucdata, filename=path[1:], attribs=ga(dp._id))
        else:
            raise FuseOsError(ENOENT)

    def utimens(self, path, times=None):
        now = time()
        mtime,atime = times if times else (now, now)
        if self.datastore.exists(filename=path[1:]):
            dp = self.datastore.get_last_version(path[1:])
            self.datastore.update_attr(dp._id, 'st_atime', atime)
            self.datastore.update_attr(dp._id, 'st_mtime', mtime)
        else:
            raise FuseOsError(ENOENT)

        
    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)
        






    ## Null pointers for these methods will cause FUSE to raise the correct
    # errors
    mkdir = None
    rmdir = None
    symlink = None
    readlink = None





        
if __name__ == '__main__':
    if len(argv) != 3:
        print "Usage: %s <database> <mountpoint> " % argv[0]
        exit(1)

    logging.getLogger().setLevel(logging.DEBUG)
    FUSE(KongFs("localhost",27017, argv[1]), argv[2], foreground=True)
