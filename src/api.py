import cgi
import os
import time
import re
import logging
import const
import urllib
import traceback
import random
import math
import operator
from django.utils import simplejson

from datetime import timedelta
from datetime import datetime

from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.api import urlfetch
from google.appengine.api import memcache

from models import *
import models.counter

def update_track_info(video, track):
    try:
        video.update_video_info(update_entry = False)        
    except models.Restricted:
        pass
    
    video.update_track_info(check_static = False, update_entry = False, change_state = False, track_title = track)
    
    

class VideoInfoHandler(webapp.RequestHandler):
    def post(self, source, videoid):
        track = self.request.get('track')
        username = self.request.get('username')
        
        if username:
            models.LastfmUser.get_or_insert("u%s" % username, username = username)                
        
        try:
            if source == 'youtube':
                source_constant = const.Video_Source_Youtube
                if len(videoid) != 11:
                    raise StandardError, "Wrong youtube videoid"            
            else:
                raise StandardError, "Unknown source: %s" % source
            
            video = Video.get_by_key_name("v%s" % videoid)
            
            if video is None:
                video = Video(key_name = "v%s" % videoid,
                              source   = source_constant,
                              videoid  = videoid,
                              added_by = const.Added_By_Hands,
                              status   = const.State_WaitingForConfirm,
                              processing_status = const.State_Processed)
                                                
                update_track_info(video, track)
                
                if video.is_static_video():
                    video.status = const.State_StaticVideoError
                
                try:                    
                    video.put()
                except:
                    video.put()
            else:
                if video.artist is None and video.status == const.State_Restricted:
                    update_track_info(video, track)
                    try:                    
                        video.put()
                    except:
                        video.put()                
            
            if video.artist:
                data = {'artist':video.artist.name, 'track':video.track}
            else:
                data = {}
            
            self.response.out.write(simplejson.dumps(data))
            return True
        except:
            et,ev,etb = sys.exc_info()                                
            tb_lines = traceback.format_exc()
                
            try:
                error_text = ev.args[0]
            except:
                error_text = ""
                
            error_msg = u"%s\n%s\n%s" % (et, error_text, unicode(str(tb_lines), 'utf-8', errors='ignore'))                
            logging.error(error_msg)
                        
            self.response.out.write(simplejson.dumps({'error':error_text}))
            return True 

    def get(self, source, videoid):
        self.post(source, videoid)        
            
            
                    
application = webapp.WSGIApplication([('/api/video_info/([^\/]*)/([^\/]*)', VideoInfoHandler)],
                                       debug=True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()    
