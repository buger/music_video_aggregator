import cgi
import time
import re
import logging
import const

from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.api import urlfetch
from google.appengine.api import memcache
from google.appengine.api.labs import taskqueue

from models import *
import models.counter

class PrepareAuthorVideoIndex(webapp.RequestHandler):
    def get(self):
        batch_size = 5
        
        start_key = self.request.get("start")
        key = self.request.get("key")
        
        if key:
            authors = [Author.get(db.Key(key))]
        else:
            if start_key:
                authors = Author.all().order('__key__').filter("__key__ > ", db.Key(start_key)).fetch(batch_size)
            else:
                authors = Author.all().order('__key__').fetch(batch_size)
                counter
                
        
        for author in authors:
            author_video_index = AuthorVideoIndex.get_or_insert("avi%s" % author.username.lower(), parent=author)            
            author_video_index.status = const.State_Waiting
            author_video_index.put()
            
            # FIXME: Won't work if video count more than 1000
            author_video_index.processed  = db.GqlQuery("SELECT __key__ FROM Video WHERE author = :1 and status = :2",author,const.State_Processed).count()
            author_video_index.waiting    = db.GqlQuery("SELECT __key__ FROM Video WHERE author = :1 and status = :2",author,const.State_Waiting).count()
            author_video_index.error      = db.GqlQuery("SELECT __key__ FROM Video WHERE author = :1 and status = :2",author,const.State_Error).count()
            author_video_index.restricted = db.GqlQuery("SELECT __key__ FROM Video WHERE author = :1 and status = :2",author,const.State_Restricted).count()
            author_video_index.wrong      = db.GqlQuery("SELECT __key__ FROM Video WHERE author = :1 and status = :2",author,const.State_WrongVideo).count()
            author_video_index.artist_not_found = db.GqlQuery("SELECT __key__ FROM Video WHERE author = :1 and status = :2",author,const.State_ArtistNotFound).count()
            author_video_index.waiting_for_confirm = db.GqlQuery("SELECT __key__ FROM Video WHERE author = :1 and status = :2",author,const.State_WaitingForConfirm).count()
            author_video_index.static_video = db.GqlQuery("SELECT __key__ FROM Video WHERE author = :1 and status = :2",author,const.State_StaticVideoError).count()
            author_video_index.deleted_video = db.GqlQuery("SELECT __key__ FROM Video WHERE author = :1 and status = :2",author,const.State_Deleted).count()
            
            author_video_index.processed_vs_errors_ratio = (author_video_index.error+author_video_index.wrong+author_video_index.artist_not_found+author_video_index.waiting_for_confirm+author_video_index.static_video+author_video_index.deleted_video) - author_video_index.processed
            
            author_video_index.status = const.State_Processed 
            author_video_index.put() 
        
        if len(authors) == batch_size:            
            task = taskqueue.Task(url='/report/prepare', method='GET', params={'start':authors[-1].key()})
            task.add("default")            
                
        return True            
                        
application = webapp.WSGIApplication([('/report/prepare', PrepareAuthorVideoIndex)],
                                       debug=True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()   