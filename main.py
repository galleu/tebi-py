import requests
import mimetypes
import hashlib

class Tebi:
    def __init__(self, bucket, **kwargs):
        self.bucket = "https://" + bucket
        self.auth = kwargs.get('auth', None)
        if (self.auth):
            self.auth = "TB-PLAIN " + self.auth


    def GetObject(self, key):
        headers = {}
        if (self.auth):
            headers["Authorization"] = self.auth
        response = requests.get(self.bucket+"/"+key, headers=headers)
        return response
        
    def PutObject(self, key, obj, **kwargs):
        file = kwargs.get('file', None)
        mime = kwargs.get('ContentType', None)
        auth = kwargs.get('auth', self.auth)
        
        CacheControl = kwargs.get('CacheControl', None)

        data = obj        
        if (mime != None and mime == "auto" and file != None):
            mime =  mimetypes.guess_type(file)[0]
        
        headers = {}
        if (mime != None):
            headers["Content-Type"] = mime

        if (CacheControl != None):
            headers["Cache-Control"] = CacheControl

        if (self.auth):
            headers["Authorization"] = auth

        if (file and not data):
            data = open(file, "rb")
            
        headers["Content-MD5"] = hashlib.md5(data).hexdigest()

        response = requests.put(self.bucket + +"/"+key, headers=headers)
        return response


    def ListObjects(self, key, **kwargs):
        auth = kwargs.get('auth', self.auth)
        headers = {
            "Authorization": auth
        }
                
        response = requests.get(self.bucket+"/?"+key, headers=headers)
        return response
