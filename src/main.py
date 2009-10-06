import cgi
import os
import time
import random
import const
import logging
import string
import urllib

from google.appengine.ext import webapp

from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext.webapp import template
from google.appengine.ext import db
from google.appengine.api import users
from google.appengine.api.labs import taskqueue


from models import *

import models.counter

from appengine_utilities.sessions import Session

import pylast

import gdata.urlfetch
import gdata.youtube
import gdata.youtube.service

gdata.service.http_request_handler = gdata.urlfetch

yt_service = gdata.youtube.service.YouTubeService()    


def doRender(handler, tname='index.html', values={}, options = {}):
    temp = os.path.join(
        os.path.dirname(__file__),
        'templates/' + tname)
    if not os.path.isfile(temp):
        return False
    
    # Make a copy of the dictionary and add the path
    newval = dict(values)
    newval['path'] = handler.request.path
    newval['const'] = const
    
    handler.session = Session()
    
    try:
        newval['username'] = handler.session['username']        
    except KeyError:
        pass
    
    newval['is_admin'] = True #users.is_current_user_admin()
     
#    handler.session = Session()
#        newval['username'] = handler.session['username']
#    if 'username' in handler.session:
                    
    outstr = template.render(temp, newval)
                
    if 'render_to_string' in options:
        return outstr
    else:
        handler.response.out.write(outstr)
        return True

class ArtistHandler(webapp.RequestHandler):
    def get(self, username):
        username = urllib.unquote_plus(urllib.unquote(username))
        
        username = unicode(username, 'utf-8', errors='ignore')
    
        artist = Artist.get_by_key_name("a%s" % username.lower())        
        videos = Video.gql("WHERE artist = :1 and status = :2 ORDER BY published_at DESC", artist, const.State_Processed).fetch(30)         
        
        doRender(self, "_artist.html", {'artist':artist, 'videos':videos})
        

class AuthorHandler(webapp.RequestHandler):
    def get(self, username, video_state = "processed"):
        author = Author.get_by_key_name("a%s" % username.lower())
        
        if author:
            artists = [entry.artist for entry in author.artists]
        else:
            artists = []
            
        for artist in artists:
            if artist.video_count == 0:
                artists.remove(artist)
            
        state = None
        
        if video_state == "processed":
            state = const.State_Processed
        elif video_state == "waiting":
            state = const.State_Waiting
        elif video_state == "error":
            state = const.State_Error
        elif video_state == "restricted":
            state = const.State_Restricted
        elif video_state == "wrong":
            state = const.State_WrongVideo
        elif video_state == "not_found":
            state = const.State_ArtistNotFound
        elif video_state == "confirm":
            state = const.State_WaitingForConfirm
        elif video_state == "static":
            state = const.State_StaticVideoError
        elif video_state == "deleted":
            state = const.State_StaticVideoError                          
        
        if state is not None:
            videos = Video.gql("WHERE author = :1 and status = :2", author, state)
        else:
            videos = None
        
        doRender(self, "_author.html", {'author':author, 'artists':artists, 'videos':videos})                        


class AuthorsHandler(webapp.RequestHandler):
    def get(self):        
        PAGESIZE = 20
        
        page = self.request.get('page')        
        try:
            page = int(page)
        except ValueError:
            page = 1
                    
        next = None
        
        order_param = self.request.get('order')            
        
        if order_param:
            if order_param == 'p_vs_e':
                order = "-processed_vs_errors_ratio"
            elif order_param == 'processed':
                order = "-processed"
            elif order_param == 'waiting':
                order = "-waiting"
            elif order_param == 'error':
                order = "-error"
            elif order_param == 'wrong':
                order = "-wrong"
            elif order_param == 'restricted':
                order = "-restricted"
            elif order_param == 'artist_not_found':
                order = "-artist_not_found"
            elif order_param == 'confirm':
                order = "-waiting_for_confirm"
            elif order_param == 'static':
                order = "-static_video"
            elif order_param == 'deleted':
                order = "-deleted_video"                                                  
            else:
                order = "-processed"
                                                                                                                                                
            author_video_indexes = AuthorVideoIndex.all().order(order).fetch(PAGESIZE + 1, (page-1)*PAGESIZE)
            authors = [avi.parent() for avi in author_video_indexes]                            
        else:
            authors = Author.all().fetch(PAGESIZE + 1, (page-1)*PAGESIZE)        
                
        
        if len(authors) == PAGESIZE + 1:
            next = page + 1
        
        authors = authors[:PAGESIZE]        
        
        previous = None
        if page > 1:
            previous = page - 1
  
        doRender(self, "_authors.html", {'authors': authors, 'next':next, 'previous': previous, 'order': order_param})
    
    def post(self):
        author_username = self.request.get('author')
        
        if author_username.isspace() is False:                  
            author = Author.get_by_key_name("a%s" % author_username.strip().lower())
            
            if author is None:
                author = Author(key_name = "a%s" % author_username.strip().lower(), status = const.State_Waiting, username = author_username)
                author.put()
                try:
                    author.initial_update()
                except:
                    pass
            
        
        self.redirect('/authors')


class SignInHandler(webapp.RequestHandler):    
    def post(self):
        username = self.request.get('username')
        
        if username.isspace() is False:
            user = User.get_by_key_name("u%s" % username.lower())
            
            if user is not None:
                self.session = Session()
                self.session['username'] = user.login                
                self.redirect("/")
            else:
                error_msg = "User not found"
                doRender(self, "_signin.html", {'error_msg': error_msg})
        else:        
            error_msg = "User can't be blank"
            doRender(self, "_signin.html", {'error_msg': error_msg})            
                    
        
    def get(self):
        doRender(self, "_signin.html")

class SignUpHandler(webapp.RequestHandler):
    def post(self):
        try:
          login = self.request.get('username')        
          user = User.get_by_key_name("u%s" % login.lower())        
          
          if user is None:
              self.session = Session()
                                      
              user = User(key_name = "u%s" % login.lower(), login=login)
              user.put()
                                                  
              self.session['username'] = user.login
              
              self.redirect("/")
          else:
              doRender(self, "_signup.html", {'error_msg' : "User already exist"})
        except:
            doRender(self, "_signup.html", {'error_msg' : "Error occurred while creating user"})
    
    def get(self):
        doRender(self, "_signup.html")

class LogOutHandler(webapp.RequestHandler):
    def get(self):
        self.session = Session()
        
        self.session.delete()
        self.redirect("/")

class TagsListHandler(webapp.RequestHandler):
    def get(self):
        tags = TagIndex.all().order("-rating").fetch(50)        
        
        max_rank = tags[0].rating
        
        template_values = {
          'tags': tags,
          'max_rank': max_rank
        }                   
                
        doRender(self, "_tags.html", template_values)
        
class TagHandler(webapp.RequestHandler):
    def get(self, tag):
        tag = urllib.unquote_plus(urllib.unquote(tag))        
        tag = unicode(tag, 'utf-8', errors='ignore')

                        
        artists = Artist.gql("WHERE tags = :1 ORDER BY last_plays DESC", tag).fetch(100)        
        
        for artist in artists:
            if artist.video_count == 0 or artist.video_count == None:
                artists.remove(artist)
        
        videos = []
        
        for artist in artists:
            video = Video.gql("WHERE artist = :1 and status = :2 ORDER BY created_at desc", artist, const.State_Processed).get()
            
            if video:
                videos.append(video)
        
        template_values = {
          'videos' : videos,
          'artists' : artists
        }   
                
        doRender(self, "_tag.html", template_values)        

           
class MainPage(webapp.RequestHandler):
    def get(self):
        outstr = memcache.get("page_main")    
        
        if outstr is None:
            videos = Video.all().order("-created_at").filter("status = ", const.State_Processed).fetch(100)
            
            artists = [video.artist for video in videos]
            
            tags_dict = {}                
            for artist in artists:
                index = 0
                tags_len = len(artist.tags)
                for tag in artist.tags:
                    if tag not in tags_dict:                    
                        tags_dict[tag] = 0
                        
                    tags_dict[tag] += (tags_len-index)                
                    index += 1
            
            items = tags_dict.items()
            
            if len(items) != 0:
                # Sorting by value
                items.sort(lambda x,y: y[1]-x[1])
                max_tag_value = items[0][1]
                
                tags = [TagForTemplate(tag[0], tag[1], max_tag_value) for tag in items]
                tags = tags[0:20]
            else:
                tags = []
                            
            template_values = {
              'tags': tags,
              'artists': artists,
              'videos': videos
            }                
                        
            outstr = doRender(self, "_home.html", template_values, {'render_to_string':True})
            memcache.set("page_main", outstr, 60)
            
        self.response.out.write(outstr)
        return False
        
  
    def post(self):
        author_username = self.request.get('author')
        
        if author_code.isspace() is False:
            author = Author.get_or_insert("a%s" % author_username.lower(), status = const.State_Waiting, username = author_username)
            logging.info("Initial author key: %s" % author.key())
            author.initial_update()
            
        self.redirect('/')    


class ReprocessVideos(webapp.RequestHandler):
    def post(self, author_key):
        videos = Video.gql("WHERE author = :1", db.Key(author_key)).fetch(300)
        
        for video in videos:
            video.status = const.State_Waiting
            video.processing_status = const.State_Waiting
            
        db.put(videos)    
    
class AddToProcessingQueue(webapp.RequestHandler):
    def post(self, type, object_key):
        if type == 'video':
            object = Video.get(object_key)
        elif type == 'author':
            object = Author.get(object_key)
            object.start_index = 0
        else:
            object = None
        
        if object:
            try:
                object.status = const.State_Waiting
                object.processing_status = const.State_Waiting
                object.put()
            except:
                et,ev,eb = sys.exc_info()
                logging.error("Error while updating video state: %s" % ev)
                
                self.response.out.write("{'status':'failed', 'message':'Error occurred'}")
                return True
                                    
            self.response.out.write("{'status':'success'}")
            return True        
        else:
            self.response.out.write("{'status':'failed', 'message':'Object not found'}")
            return True
        
    def get(self):
        pass


class BanHandler(webapp.RequestHandler):
    def post(self, type, object_key):
        if type == 'video':
            object = Video.get(object_key)
            
            status = self.request.get('status')
            if status == 'static':
                status = const.State_StaticVideoError
            else:
                status = const.State_WrongVideo
                
            object.status = status
        elif type == 'author':
            object = Author.get(object_key)
            object.status = const.State_WrongAuthor                                                                                                                                    
        else:
            object = None
        
        if object:
            try:
                object.put()
                
                if type == 'author':
                    videos = Video.gql("WHERE author = :1 and status != :2", object, const.State_StaticVideoError).fetch(999)
                    
                    for video in videos:
                        video.status = const.State_StaticVideoError
                    
                    start_at = self.request.get('start_at')                    
                    
                    if start_at:
                        start_at = int(start_at)
                        
                        videos_for_processing = videos[start_at:start_at+300]
                        
                        db.put(videos_for_processing)
                        
                        taskqueue.add(url='/report/prepare', params={'key': object_key}, method = 'GET')
                    else:
                        if len(videos) > 300:
                            counter = 0
                            
                            while counter < len(videos)+999:                            
                                taskqueue.add(url='/author/add_to_ban_list/%s' % object_key, params={'start_at': counter})                                
                                counter += 300
                        else:
                            db.put(videos)
                            
                            taskqueue.add(url='/report/prepare', params={'key': object_key}, method = 'GET')
            except:
                et,ev,eb = sys.exc_info()
                logging.error("Error while updating video state: %s, %s" % (et, ev))
                
                self.response.out.write("{'status':'failed', 'message':'Error occurred'}")
                return True
                                    
            self.response.out.write("{'status':'success'}")
            return True        
        else:
            self.response.out.write("{'status':'failed', 'message':'Object not found'}")
            return True
    
    def get(self, type, object_key):
        self.post(type, object_key)  
    
    
application = webapp.WSGIApplication(
                                     [('/', MainPage),
                                      ('/signup', SignUpHandler),
                                      ('/signin', SignInHandler),
                                      ('/logout', LogOutHandler),
                                      ('/tags', TagsListHandler),
                                      ('/tag/([^\/]*)', TagHandler),                                      
                                      ('/authors\/?', AuthorsHandler),
                                      ('/authors/artist/([^\/]*)', ArtistHandler),                                                                            
                                      ('/authors/([^\/]*)\/?', AuthorHandler),
                                      ('/authors/([^\/]*)/([^\/]*)', AuthorHandler),
                                      ('/(.*)/add_to_processing_queue/(.*)', AddToProcessingQueue),
                                      ('/(.*)/add_to_ban_list/(.*)', BanHandler),
                                      ('/author/reprocess_videos/(.*)', ReprocessVideos)],
                                     debug=True)

def main():
  run_wsgi_app(application)

if __name__ == "__main__":
  main()
