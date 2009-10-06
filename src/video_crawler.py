import cgi
import os
import re
import logging
import const
import urllib
import traceback
import random
import math
import operator
 
import time
import datetime

from datetime import timedelta

from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.api import urlfetch
from google.appengine.api import memcache
from google.appengine.api.labs import taskqueue

import gdata.base
import gdata.base.service
import gdata.youtube
import gdata.youtube.service
import gdata.urlfetch

gdata.service.http_request_handler = gdata.urlfetch

yt_service = gdata.youtube.service.YouTubeService()
g_service = gdata.base.service.GBaseService()

from models import *
import models.counter

import pylast

              
class VideoSourceCrawler(webapp.RequestHandler):
    ITEMS_PER_PAGE = 20
    
    def get(self):
        update_videos = self.request.get('update_videos')
        
        if update_videos != "":
            author = Author.gql("WHERE status = :1 and updated_at < :2", const.State_Processed, datetime.datetime.today()-timedelta(hours=6)).get()            
        else:
            author = Author.gql("WHERE status = :1", const.State_Waiting).get()                                        
        
        def tnx(key):
            author = Author.get(key)
            if author.status != const.State_Processing:
                author.status = const.State_Processing
            else:
                return False
                                                
            author.put()            
            return True
        
        if author is None:
            return True #Task complete     
        
        can_process = db.run_in_transaction(tnx, author.key()) 
        
        if update_videos != "":
            author.start_index = 0
  
        if can_process and author.start_index < 950:            
            query = gdata.youtube.service.YouTubeVideoQuery()            
            query.orderby = 'published'
            query.racy = 'include'
            query.author = author.username
            query.max_results = 30
            
            #author.start_index = 300
            
            if author.start_index != 0:
                query.start_index = author.start_index

#            raise StandardError, author.start_index
            
            if author.start_index is None:
                author.start_index = 0
                
            author.start_index += 30                        
            
            try:                                                                                                                                                                                                    
                entries = yt_service.YouTubeQuery(query).entry
            except:                
                author.status = const.State_Waiting
                author.put()
                
                if update_videos == "":
                    task = taskqueue.Task(url='/process/sources', method='GET')
                    task.add("default")                    
                else:
                    task = taskqueue.Task(url='/process/sources?update_videos=true', method='GET')
                    task.add("default")
                    
                return True
                
            if len(entries) < 30:
                author.status = const.State_Processed
            
            logging.debug("entries: %d" % len(entries))
            for entry in entries:
                videoid = None
                
                for link in entry.link:                    
                    if link.rel == "alternate":
                        videoid = re.sub('.*\?v=','',link.href)                        
                        videoid = re.sub('&.*','',videoid)                                        

                if Video.get_by_key_name("v%s" % videoid) is None:
                    published_str = entry.published.text
                                                
                    published_date = datetime.datetime(*time.strptime(published_str, "%Y-%m-%dT%H:%M:%S.000Z")[:6])  
                    
                    logging.info("Published at: %s" % str(published_date))                          
                                                    
                    video = Video(key_name = "v%s" % videoid, 
                                  videoid = videoid,
                                  source = const.Video_Source_Youtube)
                    
                    video.status = const.State_Waiting                
                    video.author = author                    
                    
                    try:
                        video.put()
                    except:
                        video.put()
                                        
                    try:
                        video.update_video_info(entry)
                    except:                
                        et,ev,etb = sys.exc_info()
                        
                        try:
                            ev = ev.args[0]
                        except:
                            ev = ""
                                                        
                        tb_lines = traceback.format_exc()
                        error_msg = None
                        
                        if et == Restricted:
                            video.status = const.State_Restricted
                            error_msg = "%s" % ev
                            logging.error("%s" % ev)
                        elif et == WrongVideo:
                            video.status = const.State_WrongVideo                
                            logging.warning("This is wrong music video: %s" % video.videoid)
                        elif et == ArtistNotFound:
                            video.status = const.State_ArtistNotFound
                            logging.warning("Artist not found: %s" % video.videoid)
                        else:
                            video.status = const.State_Error
                            
                            try:
                                error_text = ev.args[0]
                            except:
                                error_text = ""
                                                            
                            error_msg = "%s\n%s\n%s" % (et,error_text, tb_lines)
                            logging.error(error_msg)
                                            
                        if error_msg:
                            try:
                                video.error_msg = unicode(error_msg, 'utf-8', errors='ignore')
                            except:
                                video.error_msg = error_msg
                                                                
                        video.put()
                                                   
                        
        if (can_process and author.start_index >= 950) or update_videos != "":
            author.status = const.State_Processed
    
        author.put()
        
        if update_videos == "":
            task = taskqueue.Task(url='/process/sources', method='GET')
            task.add("default")                    
        else:            
            task = taskqueue.Task(url='/process/sources?update_videos=true', method='GET')
            task.add("default")
            
        return True        


class NothingFound(Exception):
    pass

class VideoCrawler(webapp.RequestHandler):
    def get(self):                            
        videoid = self.request.get('videoid')
        
        if videoid:
            video = Video.get_or_insert("v%s" % videoid,
                                        source = const.Video_Source_Youtube,
                                        videoid = videoid)
            video.status = const.State_Waiting                        
        else:
            status = self.request.get('status')
            
            if status:
                if status == 'processing':
                    status = const.State_Processing
                elif status == 'error':
                    status = const.State_Error
                else:
                    raise StandardError, "Wrong status type"
            else:
                status = const.State_Waiting
                
                
            video = Video.gql("WHERE processing_status = :1 and status = :2", const.State_Waiting, status).get()
        
        """
        # unkjJtMaXgU HD video!!
        #video = Video.gql("WHERE videoid = 'oxOJILd9P4k'").get()
        #video.status = const.State_Waiting
        #video.put()
        # 6eQUjDYSNro -GEO !!
        
        """
        """        
        video = Video.get_or_insert("voxOJILd9P4k", 
                                    source = const.Video_Source_Youtube,
                                    videoid = "oxOJILd9P4k")
         
        video.status = const.State_Waiting
        """        
            
        if video is None:
            self.response.out.write("Task completed; All videos processed.")
            return True
                       
        def tnx(key):
            video = Video.get(key)
            if video.processing_status != const.State_Processing:
                video.processing_status = const.State_Processing
            else:
                return False
                                                
            video.put()            
            return True
        
        if db.run_in_transaction(tnx, video.key()):                                        
            logging.info("Processing video: %s,%s" % (video.videoid, video.title))
    
            try:                         
                if videoid or video.status == const.State_Error or video.status == const.State_Processing or video.status == const.State_WrongVideo or len(video.thumbnails) == 0:
                    video.update_video_info()                        
                                                                  
                video.update_track_info()
                
                video.processing_status = const.State_Processed
                video.put()                                 
            except:
                et,ev,etb = sys.exc_info()                                
                tb_lines = traceback.format_exc()
                 
                if et == ArtistNotFound:
                    video.status = const.State_ArtistNotFound
                elif et == StaticVideoError:
                    video.status = const.State_StaticVideoError
                elif et == Restricted:
                    video.status = const.State_Restricted
                else:
                    video.status = const.State_Error                            
                
                try:
                    error_text = ev.args[0]
                except:
                    error_text = ""
                    
                error_msg = u"%s\n%s\n%s" % (et, error_text, unicode(str(tb_lines), 'utf-8', errors='ignore'))                
                logging.error(error_msg)
                                
                if error_msg:
                    try:
                        video.error_msg = unicode(error_msg, 'utf-8', errors='ignore')
                    except:
                        video.error_msg = error_msg
                
                video.processing_status = const.State_Processed
                                
                video.put()
                                
        task = taskqueue.Task(url="/process/videos?status=%s" % self.request.get('status'), method='GET')
        task.add("default")                                

        return True            

class RescanVideosWithErrors(webapp.RequestHandler):
    def get(self):
        videos = Video.gql("WHERE status in (-1,1)")
        for video in videos:
            video.status = const.State_Waiting
            counter.decrement("waiting_videos")
            
        db.put(videos)
        
        html = '<html><head><meta http-equiv="refresh" content="0;URL=/process/videos" /><head></html>'
        self.response.out.write(html)
        return True            
        
class ClearDbHandler(webapp.RequestHandler):
    def get(self):        
        videos = Video.all().fetch(100)
        db.delete(videos)
        memcache.flush_all()
        if len(videos) == 0:
            artists = Artist.all().fetch(100)
            db.delete(artists)
            
            if len(artists) == 0:
                authors = Author.all().fetch(100)
                db.delete(authors)
                
                if len(authors) == 0:
                    counters = counter.Counter.all().fetch(100)
                    db.delete(counters)
                    
                    if len(counters) == 0:
                        channels = Channel.all().fetch(100)
                        db.delete(channels)
                        
                        if len(channels) == 0:
                            video_indexes = VideoIndex.all().fetch(100)
                            db.delete(video_indexes)
                                                    
                            if len(video_indexes) == 0:
                                author_artists = AuthorArtists.all().fetch(100)
                                db.delete(author_artists)
                                
                                if len(author_artists) == 0:
                                    self.response.out.write("DB clear. Task complete")
                                    return True
        
        html = '<html><head><meta http-equiv="refresh" content="0;URL=/process/cleardb"/><head></html>'
        self.response.out.write(html)
        return True

class GetRecordLabels(webapp.RequestHandler):
    def get(self):
        start_index = self.request.get('start-index')
        
        try:
            start_index = int(start_index)
        except ValueError:
            start_index = 1
        
        gb_client = gdata.base.service.GBaseService()  
        q = gdata.base.service.BaseQuery()
        
        q.feed = 'http://gdata.youtube.com/feeds/api/channels'  
        q['start-index'] = str(start_index)  
        q['max-results'] = '10'
        q['q'] = 'records'
        q['v'] = '2'  
                
        feed = gb_client.QuerySnippetsFeed(q.ToUri())
        
        for entry in feed.entry:
            if Author.get_by_key_name("a%s" % entry.author[0].name.text.lower()) is None:
                author = Author.get_or_insert("a%s" % entry.author[0].name.text.lower(), status = const.State_WaitingForConfirm, username = entry.author[0].name.text)            
                author.initial_update()
                logging.warn("Record label found %s" % entry.author[0].name.text)
        
        start_index = start_index + 10
        
        html = '<html><head><meta http-equiv="refresh" content="0;URL=/process/record_labels?start-index='+str(start_index)+'"/><head></html>'
        self.response.out.write(html)
        return True    

# Record labels from wikipedia
class GetRecordLabels2(webapp.RequestHandler):
    def get(self):
        start_index = self.request.get('start-index')        
        try:
            start_index = int(start_index)
        except ValueError:
            start_index = 0
                    
        record_labels = memcache.get("record_labels_list")
        
        try:
            gb_client = gdata.base.service.GBaseService()  
            q = gdata.base.service.BaseQuery()
            
            q.feed = 'http://gdata.youtube.com/feeds/api/channels'
            q['client'] = 'ytapi-leonidbugaev-Lostvideosfm-hjvpuers-0'
            q['key'] = 'AI39si4Nxri5jhcmN6d20gl4A1eKK63xgtw_SRNQePzBiALphr1WGF0zg5JBPLvytL0hXbHanoCczMbsbfGBSjyKswTXpib5Ig'
            q['max-results'] = '10'
            q['v'] = '2'  
    
    
            if record_labels is None:
                url = "http://gist.github.com/raw/126760/1903c3a0962edec3fcbce63c2330fd938d987ce7/gistfile1.txt"
                result = urlfetch.fetch(url)
                result = result.content.replace("\"",'')
                record_labels = result.split("\n")
                memcache.add("record_labels_list", record_labels, 600)            
             
            for record_label in record_labels[start_index:(start_index+5)]:
                q['q'] = "\"%s\"" % record_label                
                feed = gb_client.QuerySnippetsFeed(q.ToUri())
                
                if len(feed.entry) != 0:
                    index = 0
                    for entry in feed.entry:
                        username = entry.author[0].name.text.lower()
                        
                        logging.info("Found! '%s' found: %s" % (record_label, username))
                        
                        if index == 0:
                            status = const.State_Waiting
                        else:
                            status = const.State_WaitingForConfirm
                        
                        author = Author.get_or_insert("a%s" % username, status = status, username = username, search_text = record_label)                                        
                        author.initial_update()
                        
                        index += 1
                else:                
                    logging.info("Record label '%s' not found" % record_label)
                
            
            start_index += 5
            
            if start_index > len(record_labels):
                return True     
        except:
            self.response.out.write('Some errors occurred: %s %s, restarting' % (sys.exc_info()[0], sys.exc_info()[1]))     
        
        html = '<html><head><meta http-equiv="refresh" content="0;URL=/process/record_labels2?start-index='+str(start_index)+'"/><head></html>'
        self.response.out.write(html)            
        return True
    
    
class ThreadTestHandler(webapp.RequestHandler):
    def get(self):
        for i in range(10):
            logging.info('%d' % i)
            time.sleep(1)
            
        html = '<html><head><meta http-equiv="refresh" content="0;URL=/process/test_thread"/><head></html>'
        self.response.out.write(html)
        return True


class PrepareForProcessingHandler(webapp.RequestHandler):
    def get(self):
        key = self.request.get('key')
        
        if key:
            key = db.Key(key)                        
            videos = Video.all().order("__key__").filter("__key__ > ", key).fetch(200)
        else:
            videos = Video.all().order("__key__").fetch(200)
        
        for video in videos:
            video.processing_status = const.State_Waiting
        
        try:
            db.put(videos)
            next_key = str(videos[-1].key())
        except:
            next_key = key
        
        if len(videos) < 200:
            html = "Task complete:"
        else:
            html = '<html><head><meta http-equiv="refresh" content="0;URL=/process/prepare_for_processing?key='+str(next_key)+'"/><head></html>'
            
        self.response.out.write(html)
        return True

class PrepareTagIndexHandler(webapp.RequestHandler):
    def get(self):
        key = self.request.get('key')
        
        if key:
            key = db.Key(key)                        
            tags = TagIndex.all().order("__key__").filter("__key__ > ", key).fetch(100)
        else:
            tags = TagIndex.all().order("__key__").fetch(100)
        
        for tag in tags:
            tag.status = const.State_Waiting
        
        db.put(tags)
        
        if len(tags) < 100:
            html = "Task complete:"
        else:
            next_key= tags[-1].key()
            html = '<html><head><meta http-equiv="refresh" content="0;URL=/process/prepare_tag_index?key='+str(next_key)+'"/><head></html>'
            
        self.response.out.write(html)
        return True            
        

class UpdateArtistVideoIndexHandler(webapp.RequestHandler):
    def get(self):
        key = self.request.get('key')
        
        if key:
            key = db.Key(key)                        
            artists = Artist.all().order("__key__").filter("__key__ > ", key).fetch(10)
        else:
            artists = Artist.all().order("__key__").fetch(10)
        
        tags = []
        
        for artist in artists:
            videos = Video.gql("WHERE artist = :1 and status = :2 ORDER BY created_at DESC", artist, const.State_Processed).fetch(100)
                        
            artist.video_count = len(videos)
            
            if len(videos) != 0:
                artist.last_updated = videos[0].created_at            
            
            index = 0
            tags_len = len(artist.tags)
            
            db_tags = []
            for tag in artist.tags:
                db_tag = TagIndex.get_by_key_name("t%s" % tag.lower())
                
                if not db_tag:
                    db_tag = TagIndex(key_name = "t%s" % tag.lower())
                
                db_tag.name = tag
                
                if db_tag.status == const.State_Waiting:
                    db_tag.count = 0
                    db_tag.rating = 0
                    db_tag.status = const.State_Processed
                        
                db_tag.count += artist.video_count
                
                db_tag.rating += (tags_len-index) * artist.video_count               
                index += 1
                
                db_tags.insert(0, db_tag)
                
            db.put(db_tags)
                
            
        db.put(artists)
        
        
        if len(artists) < 10:
            html = "Task complete:"
        else:
            next_key = artists[-1].key()
            html = '<html><head><meta http-equiv="refresh" content="0;URL=/process/update_artist_video_index?key='+str(next_key)+'"/><head></html>'
            
        self.response.out.write(html)
        return True        
    
class CheckStaticVideosHandler(webapp.RequestHandler):
    def get(self):
        video = Video.gql("WHERE processing_status = :1", const.State_Waiting).get()
        
        if video is None:
            self.response.out.write("Task completed; All videos processed.")
            return True
        
        if video.status == const.State_WrongVideo or video.status == const.State_Restricted:
            video.processing_status = const.State_Processed
            video.put() 
            
            html = '<html><head><meta http-equiv="refresh" content="0;URL=/process/check_dublication"/><head></html>'
            self.response.out.write(html)
            return True    
        
        def tnx(key):
            video = Video.get(key)
            
            logging.info("Video state: %s, %s" % (video.processing_status, key))
            
            if video.processing_status != const.State_Processing and video.processing_status != const.State_Processed:
                video.processing_status = const.State_Processing
            else:
                return False
                                                            
            video.put()            
            return True
        
        tr = db.run_in_transaction(tnx, video.key())
        
        logging.info("Transaction result: %s" % tr)
        
        if tr:                                        
            logging.info("Processing video: %s,%s" % (video.videoid, video.title))
            
            try:
                is_static_video = video.is_static_video()
                
                if is_static_video:
                    logging.error("Video static error %s" % video.videoid)
                    video.status = const.State_StaticVideoError                    
                    video.processing_status = const.State_Processed
                    video.put()                                                                        
            except:
                et,ev,etb = sys.exc_info()                                
                tb_lines = traceback.format_exc()                
                logging.error("Error occuired: %s, %s" % (et, ev))
                video.processing_status = const.State_Error
                video.put()
            
        html = '<html><head><meta http-equiv="refresh" content="0;URL=/process/check_dublication"/><head></html>'
        self.response.out.write(html)
        return True    


class FixProcessingState(webapp.RequestHandler):
    def get(self):
        key = self.request.get('key')
        
                        
        authors = Author.all().filter("status = ", const.State_Processing).fetch(200)

        for author in authors:
            author.status = const.State_Waiting
            author.processing_state = const.State_Waiting
            
        db.put(authors)
        
        if len(authors) < 200:
            html = "Task Complete"
        else:                                
            html = '<html><head><meta http-equiv="refresh" content="0;URL=/process/fix_processing_state"/><head></html>'
        self.response.out.write(html)
        return True 
    
class FixVideoidPrefix(webapp.RequestHandler):
    def get(self):
        offset = self.request.get('offset')
        date = self.request.get('date')
        
        if date:
            pass
        else:
            date = '2009-09-16 09:40:32'
            
        if offset:
            offset = int(offset)
        else:
            offset = 0                              
                        
        
        videos = Video.gql("WHERE created_at > DATETIME('"+date+"') ORDER BY created_at").fetch(10, offset)
            
        if offset == 1000: 
            date = (videos[-1].created_at - timedelta(seconds=1) ).strftime("%Y-%m-%d %H:%M:%S")
                    
            html = '<html><head><meta http-equiv="refresh" content="0;URL=/process/fix_videoid_prefix?offset='+str(0)+'&date='+date+'"/><head></html>'
            self.response.out.write(html)
            return True 
        
        if len(videos) == 0:
            html = "Task Complete"
            self.response.out.write(html)
            return True                                    
                    
        for video in videos:
            logging.warning("Videoid: %s, %s" % (video.videoid, str(video.created_at)))
            if re.search('youtube', video.videoid):                                
                videoid = re.sub('&.*','',video.videoid)

                logging.info("processed videoid: %s" % videoid)
                                                
                new_video = Video.get_or_insert("v%s" % videoid,
                                  videoid  = videoid,
                                  title    = video.title,
                                  description = video.description,
                                  artist   = video.artist,
                                  track    = video.track,
                                  author   = video.author,
                                  duration = video.duration,
                                  source   = video.source,
                                  contents = video.contents,
                                  thumbnails = video.thumbnails,
                                  rating   = video.rating,
                                  votes    = video.votes,
                                  added_by = video.added_by,
                                  status   = video.status,
                                  processing_status = video.processing_status,
                                  geo      = video.geo,
                                  noembed  = video.noembed,
                                  hd_version = video.hd_version,
                                  live_version = video.live_version,
                                  created_at = video.created_at,
                                  error_msg = video.error_msg
                                  )        
                
                db.delete(video)
        
        
        offset = offset+10
        
        html = '<html><head><meta http-equiv="refresh" content="0;URL=/process/fix_videoid_prefix?offset='+str(offset)+'&date='+date+'"/><head></html>'
        self.response.out.write(html)
        return True        
        
     
# Some changes for Video in DB   
#    published_at
#    check for deleted state
#    live video
#    counter
class MigrateDB01(webapp.RequestHandler):
    def get(self):
        batch_size = 10
        
        key = self.request.get('key')
        
        if key:
            videos = db.GqlQuery("SELECT * FROM Video WHERE __key__ > :1 AND status = :2 ORDER BY __key__", db.Key(key), const.State_Processed).fetch(batch_size)
        else:
            videos = db.GqlQuery("SELECT * FROM Video WHERE status = :1 ORDER BY __key__", const.State_Processed).fetch(batch_size)                
        
        
        for video in videos:
            try:        
                entry = yt_service.GetYouTubeVideoEntry(video_id = video.videoid)
            except:        
                et,ev,etb = sys.exc_info()                
                if re.search('404L', str(ev)):
                    logging.info("Founded deleted video")
                    video.status = const.State_Deleted
                elif re.search('Invalid id', str(ev)):
                    logging.critical("Invalid id found: %s" % video.videoid)
                else:
                    raise StandardError, "Can't get youtube entry: %s" % str(ev)
            
            if video.status != const.State_Deleted:
                published_str = entry.published.text                                    
                published_date = datetime.datetime(*time.strptime(published_str, "%Y-%m-%dT%H:%M:%S.000Z")[:6])        
                video.published_at = published_date
                
                if entry.media.keywords is None:
                    keywords = ""
                else:
                    keywords = entry.media.keywords.text        
                    if keywords is None:
                        keywords = ""
                
                if video.description is None:
                    video.description = ""
                
                if video.title is None:
                    raise StandardError, video.title
            
                if re.search('\Wcover', video.title.lower()) or re.search('\Wcover', video.description.lower()) or re.search('\Wcover', keywords.lower()):
                    self.cover_version = True 
                    logging.info("Cover video found")                                    
        
        db.put(videos)
        
        if len(videos) == batch_size:
            task = taskqueue.Task(url='/process/migrate_db_01', method='GET', params={'key':videos[-1].key()})
            task.add("update-videos")            

        return True         

                    
class ProcessedVideosCount(webapp.RequestHandler):
    def get(self):
        batch_range = 400
        
        key = self.request.get('key')
        count = self.request.get('count')
        
        if count:
            count = int(count)
        else:
            count = 0
                                
        if key:
            videos = db.GqlQuery("SELECT __key__ FROM Video WHERE __key__ > :1 AND status = :2 ORDER BY __key__", db.Key(key), const.State_WaitingForConfirm).fetch(batch_range)
        else:
            videos = db.GqlQuery("SELECT __key__ FROM Video WHERE status = :1 ORDER BY __key__", const.State_WaitingForConfirm).fetch(batch_range)
        
        count += len(videos)
        
        if len(videos) < batch_range:
            html = "Task Complete; Processed videos count: %d" % count
        else:                                
            html = '<html><head><meta http-equiv="refresh" content="0;URL=/process/videos_count?key='+str(videos[-1])+'&count='+str(count)+'"/><head></html>'
            
        self.response.out.write(html)
        return True         
        
application = webapp.WSGIApplication([('/process/sources', VideoSourceCrawler),
                                      ('/process/videos', VideoCrawler),
                                      ('/process/cleardb', ClearDbHandler),
                                      ('/process/rescan_videos', RescanVideosWithErrors),
                                      ('/process/test_thread', ThreadTestHandler),
                                      ('/process/record_labels', GetRecordLabels),
                                      ('/process/record_labels2', GetRecordLabels2),
                                      ('/process/check_static_videos', CheckStaticVideosHandler),
                                      ('/process/update_artist_video_index', UpdateArtistVideoIndexHandler),
                                      ('/process/prepare_for_processing', PrepareForProcessingHandler),
                                      ('/process/prepare_tag_index', PrepareTagIndexHandler),
                                      ('/process/fix_processing_state', FixProcessingState),
                                      ('/process/videos_count', ProcessedVideosCount),
                                      ('/process/migrate_db_01', MigrateDB01),
                                      ('/process/fix_videoid_prefix', FixVideoidPrefix)],
                                       debug=True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()    
