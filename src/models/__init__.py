import const
import sys
import models.counter
import logging
import re
import pylast
import math
import operator

import time
import datetime

from google.appengine.ext import db
from google.appengine.api import urlfetch
from google.appengine.api import memcache
from google.appengine.api import images

import gdata.youtube
import gdata.youtube.service
import gdata.urlfetch

gdata.service.http_request_handler = gdata.urlfetch
yt_service = gdata.youtube.service.YouTubeService()     

# Array util function    
def flatten(l):
    if isinstance(l,list):
        return sum(map(flatten,l),[])
    else:
        return [l]

class Restricted(Exception):
    pass

class WrongVideo(Exception):
    pass                    

class Author(db.Model):
    # Record label name used in video_crawler record_labels2
    search_text = db.StringProperty()
    
    username    = db.StringProperty(required = True)
    username_upper = db.StringProperty()
    title       = db.StringProperty()
    description = db.TextProperty()
    link        = db.LinkProperty()
    thumbnail   = db.StringProperty()  
    
    pages_waiting_for_processing = db.ListProperty(int)
        
    status      = db.IntegerProperty()
    processing_status = db.IntegerProperty()
    
    error_msg   = db.TextProperty()
    start_index = db.IntegerProperty()
    
    created_at  = db.DateTimeProperty(auto_now_add = True)
    updated_at  = db.DateTimeProperty(auto_now = True)
        
    @property
    def video_index(self):        
        video_index_object = memcache.get('avi%s' % self.username.lower())
        
        if video_index_object is None:        
            video_index_object = AuthorVideoIndex.get_by_key_name("avi%s" % self.username.lower(), parent = self)        
        
            if video_index_object is None:
                video_index_object = AuthorVideoIndex()
            
            if not memcache.set('avi%s' % self.username.lower(), video_index_object, 360):
                logging.error("Memcache set failed")
            
        return video_index_object            
    
    def initial_update(self):                
        try:                         
            user_entry = yt_service.GetYouTubeUserEntry(username=self.username)
                       
            self.username = user_entry.username.text
            self.username_upper = user_entry.username.text.upper()
            
            for link in user_entry.link:
                if link.rel == 'related':
                    self.link = link.href
          
            if user_entry.title:
                self.title = unicode(user_entry.title.text, 'utf-8', errors='ignore')
          
            if user_entry.description:
                self.description = unicode(user_entry.description.text, 'utf-8', errors='ignore')
            
            self.status = const.State_Waiting
            self.start_index = 0            
            self.put()                        
            
        except urlfetch.DownloadError:   
            self.username_upper = self.username.upper()       
            self.status = const.State_Error
            self.error_msg =  "Can't connect to Youtube"
            self.put()  
            
class AuthorVideoIndex(db.Model):
    processed  = db.IntegerProperty()
    waiting    = db.IntegerProperty()
    error      = db.IntegerProperty()
    restricted = db.IntegerProperty()
    wrong      = db.IntegerProperty()
    artist_not_found = db.IntegerProperty()
    waiting_for_confirm = db.IntegerProperty()
    processed_vs_errors_ratio = db.IntegerProperty()
    static_video = db.IntegerProperty()
    deleted_video = db.IntegerProperty()
    cover      = db.IntegerProperty()
    status     = db.IntegerProperty()                   


class Artist(db.Model):
    name        = db.StringProperty()
    description = db.TextProperty()
    link        = db.StringProperty()
    thumbnails  = db.StringListProperty()    
    tags        = db.StringListProperty()
    last_plays  = db.IntegerProperty()
    last_listeners = db.IntegerProperty()
    status      = db.IntegerProperty()
    
    video_count  = db.IntegerProperty()
    last_updated = db.DateTimeProperty()
    
    def pretty_tags(self):
        return (", ".join(self.tags[0:2])).lower()
    
    @property
    def link(self):
        return re.sub(' ', '+', "/authors/artist/%s" % self.name)
    
    @property
    def lastfm_link(self):
        return re.sub(' ', '+', "http://last.fm/music/%s" % self.name)
    
    def processed_videos(self):
        return Video.gql("WHERE artist = :1 and status = :2", self, const.State_Processed)

# For templates, using in tag clouds
class TagForTemplate:
    def __init__(self, tag, value, max_value):
        self.name = tag
        self.value = value
        self.max_value = max_value
        
    def font_size_in_em(self):
        size = float(self.value)/self.max_value 
        
        if size < 0.2:
            size = 0.2
            
        return size
    
class TagIndex(db.Model):
    name = db.StringProperty()
    count = db.IntegerProperty(default = 0)
    rating = db.IntegerProperty(default = 0)
    
    status = db.IntegerProperty(default = const.State_Waiting)
    
    def font_size(self, rank):
        self.rating/rank
    
class ArtistNotFound(Exception):
    pass      

class StaticVideoError(Exception):
    pass                                                               

class Video(db.Model):
    videoid     = db.StringProperty(required = True)
    title       = db.StringProperty()
    description = db.TextProperty() 
    artist      = db.ReferenceProperty(Artist)
    track       = db.StringProperty()
    author      = db.ReferenceProperty(Author)
    duration    = db.IntegerProperty()    
    source      = db.IntegerProperty(required = True)
    contents    = db.StringListProperty()
    thumbnails  = db.StringListProperty()
    rating      = db.IntegerProperty()
    votes       = db.IntegerProperty()
    
    added_by    = db.IntegerProperty() #Hands or Robot :)
    status      = db.IntegerProperty()
    processing_status = db.IntegerProperty(default = const.State_Waiting)
              
    geo         = db.GeoPtProperty()
    noembed     = db.BooleanProperty()
    hd_version  = db.BooleanProperty(default = False)    
    live_version  = db.BooleanProperty(default = False)
    cover_version  = db.BooleanProperty(default = False)
    
    created_at  = db.DateTimeProperty(auto_now_add = True)
    published_at = db.DateTimeProperty()
    
    error_msg   = db.TextProperty()        
    
    def tiny_title(self):
        if len(self.title) > 35:
            return "%s..." % self.title[0:35]
        else:
            return self.title
    
    def song_title(self):
        _title = "%s &mdash; %s" % (self.artist.name, self.track)
        
        if len(_title) > 40:
            return "%s..." % _title[0:40]
        else:
            return _title         
    
    def thumbnail(self):
        if len(self.thumbnails) != 0:
            return self.thumbnails[1]
        else:
            return ""
    
    def link(self):
        return "http://www.youtube.com/watch?v=%s" % self.videoid
    
    def guess_artist_and_track(self):        
        search_text = self.title.lower()        
        
        search_text = re.sub('on\schannel.*', '', search_text)
        search_text = re.sub('from the.*', '', search_text)
        search_text = re.sub('from .*', '', search_text)
        search_text = re.sub('live in.*', '', search_text)
        search_text = re.sub('live at.*', '', search_text)
        search_text = re.sub('live on.*', '', search_text)
        if self.author:            
            search_text = re.sub(self.author.title.lower(),'',search_text)
        
        #Artist - "Track" in Poland            
        matcher = re.search('(.*\".*\").*', search_text)        
        if matcher and matcher.group(1):
            search_text = matcher.group(1)            
                                              
        search_text = re.sub('\(.*\)', '', search_text)
        search_text = re.sub('\[.*\]', '', search_text)
        search_text = re.sub('\!', '', search_text)        
        search_text = re.sub('\"', ' ', search_text)
        search_text = re.sub(':', ' ', search_text)                        
        search_text = re.sub('\/', ' ', search_text)
        
        #For They Shoot Music
        search_text = re.sub('they shoot music', '', search_text)
        
        search_text = re.sub('sun studio sessions', '', search_text)
        
        search_text = re.sub('on q tv', '', search_text)
        search_text = re.sub('on qtv', '', search_text)
        
        #For labloque
        search_text = re.sub('\#[\d\.]*', ' ', search_text)
        
        #For fatcat records
        search_text = re.sub('\sfatcat\s', ' ', search_text)
        
        #(Wolf - I Will Kill Again) - NEW Video Clip !!! 
        matcher = re.search('([^-]*-[^-]*).*$', search_text)
        if matcher and matcher.group(1):
            search_text = matcher.group(1)
                        
        matcher = re.search('(.*\-.*)\:', search_text)
        if matcher and matcher.group(1):
            search_text = matcher.group(1)            
            
        search_text = re.sub('\@.*', '', search_text)
        search_text = re.sub('\" for .*', '', search_text)
        search_text = re.sub('at the .*', '', search_text)
        search_text = re.sub('a history of.*', '', search_text)
        
        pattern = re.compile('\W(target|series|live|tourfilm|preview|by|remix|performs|download now|official video|out now|uk|new|upcoming video|journal|videoclip|plays|music video|commercial|tour|trailer|cover|in studio|records|video|clip|recording|update|ep)(?:\W|$)')        
        search_text = pattern.sub(' ', search_text)
        search_text = pattern.sub(' ', search_text)
        
        track_not_found = False
                    
        if len(search_text.strip().split()) > 1:
            logging.info("Searching track: %s" % search_text)
            search_results = pylast.TrackSearch('', search_text, '170909e77e67705570080196aca5040b', '516a97ba6f832d9184ae5b32c231a3af', '').get_next_page()
        else:
            logging.info("Searching artist: %s" % search_text)
            search_results = pylast.ArtistSearch(search_text, '170909e77e67705570080196aca5040b', '516a97ba6f832d9184ae5b32c231a3af', '').get_next_page()
            track_not_found = True
        
        if len(search_results) == 0:
            track_not_found = True
            matcher = None
            
            matcher = re.search('([^-]*)', search_text)
            if matcher is None:                
                matcher = re.search('([^\:]*)', search_text)
            if matcher is None:                
                matcher = re.search('(.*)\'.*\'', search_text)                            
                            
            if matcher and matcher.group(1):
                search_text_artist = matcher.group(1)                        
                logging.info("Trying to guess artist: %s" % search_text_artist)
                search_results = pylast.ArtistSearch(search_text_artist, '170909e77e67705570080196aca5040b', '516a97ba6f832d9184ae5b32c231a3af', '').get_next_page()
                if len(search_results) == 0:
                    raise ArtistNotFound, "Search text: %s" % search_text_artist
            else:
                raise ArtistNotFound, "Search text: %s" % search_text
        
        track = None
        artist_entry = None
        
        if track_not_found:
            for entry in search_results:
                if entry.get_name().lower() == search_text.strip().lower():                        
                    artist_entry = entry
            
            if artist_entry is None:
                artist_entry = search_results[0]
        else:
            track = search_results[0]
            artist_entry = track.get_artist()        
             
        logging.info("Artist name: %s" % artist_entry.get_name())
        
        thumbnails = [artist_entry.get_image_url(pylast.IMAGE_SMALL), 
                      artist_entry.get_image_url(pylast.IMAGE_MEDIUM), 
                      artist_entry.get_image_url(pylast.IMAGE_LARGE)]
                                                        
        for thumbnail in thumbnails:
            if thumbnail is None:
                thumbnails = []
        
        tags = artist_entry.get_top_tags(3)                
        tags_list = [tag.get_item().get_name() for tag in tags]
                
        artist = Artist.get_or_insert("a%s" % artist_entry.get_name().lower(), 
                                          name = artist_entry.get_name(),
                                          description = artist_entry.get_bio_content(),
                                          link = artist_entry.get_url(),
                                          thumbnails = thumbnails,
                                          tags = tags_list,
                                          last_plays = artist_entry.get_playcount(),
                                          last_listeners = artist_entry.get_listener_count()                                                                                                   
                                          )
        
            
        if track and search_text.strip() != artist.name.lower():
            track = track.get_name()
        else:
            search_text = re.sub('(:|-)', ' ', search_text)
            
            track = re.sub(artist.name.lower(),'',search_text).strip()
            if len(track) == 0:
                track = 'Unknown'
        
        track = track.title()
            
        logging.info("Trying to guess track: %s : %s" % (artist.name, track))
        
        return (artist, track)
    
    def update_track_info(self, check_static = True, update_entry = True, change_state = True, track_title = None):
        if track_title:
            self.title = track_title
                        
        artist, track = self.guess_artist_and_track()
        
        self.artist = artist
        self.track = track
        
        if re.search(artist.name.lower(), self.title.lower()) is None:
            if re.search(artist.name.lower(), self.description.lower()) is None:
                self.status = const.State_WaitingForConfirm
                logging.info("Waiting for confirm video found: %s, %s" % (self.videoid, self.title))
        
                
        if check_static and self.is_static_video():
            raise StaticVideoError
                                
        if change_state:
            self.status = const.State_Processed
            
        if update_entry:                                
            self.put()
        
        aa = AuthorArtists.get_or_insert("aa%s_%s" % (artist.name.lower(),self.author.username.lower()))
        aa.artist = artist
        aa.author = self.author
        aa.put()                
        
        counter.increment("c%s_video_count" % self.author.username)
        
        
    def is_static_video(self):
        logging.info("Checking static video")
        logging.info("Thumbnails size: %d" % len(self.thumbnails))
        
        if len(self.thumbnails) != 0:
            image1 = images.Image(db.Blob(urlfetch.fetch(self.thumbnails[1]).content))
            image2 = images.Image(db.Blob(urlfetch.fetch(self.thumbnails[2]).content))
            
            h1 = flatten(image1.histogram())
            h2 = flatten(image2.histogram())
            
            rms = math.sqrt(reduce(operator.add,
                    map(lambda a,b: (a-b)**2, h1, h2))/len(h1))
            
            logging.info("Images RMS: %d" % rms)
            # More lesser rms -> same images            
            return rms < 15
        else:
            return False      
        
    def update_video_info(self, entry = None, update_entry = True):        
        if entry is None:
            yt_service = gdata.youtube.service.YouTubeService()            
            entry = yt_service.GetYouTubeVideoEntry(video_id = self.videoid)
            
            if self.author is None:
                author_name = entry.author[0].name.text
                
                author = Author(key_name = "a%s" % author_name.lower(), 
                                status = const.State_WaitingForConfirm, 
                                username = author_name,
                                title = author_name)
                try:
                    author.put()
                except:
                    author.put()
                
                self.author = author                
                         
        
        self.title = unicode(entry.media.title.text, 'utf-8', errors='ignore')                
        
        published_str = entry.published.text                                    
        published_date = datetime.datetime(*time.strptime(published_str, "%Y-%m-%dT%H:%M:%S.000Z")[:6])        
        self.published_at = published_date        
        
        for category in entry.category:
            if category.scheme == "http://gdata.youtube.com/schemas/2007/categories.cat":
                if category.term != "Music":  
                    logging.warn("Wrong video category: %s, %s" % (self.videoid, self.title))                          
                    raise WrongVideo
                                        
        if entry.noembed is None:                    
            self.noembed = False
        else:
            self.noembed = True
            raise Restricted, "This video can't be embedded"
                            
        if entry.control and entry.control.state:
            if entry.control.state.name == "restricted":
                raise Restricted, entry.control.state.text    
                        
        
        if entry.media.description:
            try:                       
                self.description = unicode(entry.media.description.text, 'utf-8', errors='ignore')
            except:
                pass
            
                                            
        keywords = entry.media.keywords.text
        
        if keywords is None:
            keywords = ""
            
        if self.description is None:
            self.description = ""
            
        if re.search('(\Wlive\W|perform)', self.title.lower()) or re.search('(\Wlive\W|perform)', self.description.lower()) or re.search('(\Wlive\W|perform)', keywords.lower()):
            self.live_version = True 
            logging.info("Live video found")
            
        if re.search('\Wcover', self.title.lower()) or re.search('\Wcover', self.description.lower()) or re.search('\Wcover', keywords.lower()):
            self.cover_version = True 
            logging.info("Cover video found")            
                                
        if re.search('\Whd\W', self.title.lower()) or re.search('\Whd\W', self.description.lower()) or re.search('\Whd\W', keywords.lower()):
            self.hd_version = True 
            logging.info("HD video found")
        
        ban_pattern = re.compile("(how to play|episode|tourfilm|nvetv|diary|explains|lesson|journal|blogs|podcast|interview|webisode|intro|tour update|video update|coming soon|making the|epk|unknown|entervista|trailer|albumtrailer|teaser|making of|behind the scene|announcement)")
        if ban_pattern.search(self.title.lower()) or ban_pattern.search(self.description.lower()) or ban_pattern.search(keywords.lower()):
            logging.warn("Wrong words found: %s, %s" % (self.videoid, self.title))                
            raise WrongVideo                        
        
        self.thumbnails = []
        for thumbnail in entry.media.thumbnail:
            self.thumbnails.append(thumbnail.url)
        
        self.thumbnails = sorted(self.thumbnails)
                        
        self.contents = []
        if entry.media.content:                
            for content in entry.media.content:
                self.duration = int(content.duration)
                self.contents.append(content.url)
                if self.duration and self.duration < 70:
                    raise WrongVideo                                            
        
        if entry.geo:
            geo_data = entry.geo.location()                
            self.geo = '%s,%s' % (str(geo_data[0]), str(geo_data[1]))
        
        if update_entry:
            self.put()
        
    
class User(db.Model):
    login        = db.StringProperty(required = True)
    display_name = db.StringProperty() 
    login_method = db.IntegerProperty()    
    author       = db.ReferenceProperty(Author)
    created_at   = db.DateTimeProperty(auto_now_add = True)
    
    
class Channel(db.Model):
    title       = db.StringProperty()
    description = db.TextProperty()
    creator     = db.ReferenceProperty(User)
    logo        = db.StringProperty()    
    created_at  = db.DateTimeProperty(auto_now_add = True)
    updated_at  = db.DateTimeProperty(auto_now = True)
    

class AuthorArtists(db.Model):
    author      = db.ReferenceProperty(Author, collection_name = "artists")
    artist      = db.ReferenceProperty(Artist, collection_name = "authors")
        
class ChannelVideos(db.Model):
    channel     = db.ReferenceProperty(Channel)
    video       = db.ReferenceProperty(Video)
    votes       = db.IntegerProperty()    
    views       = db.IntegerProperty(default = 0)
    status      = db.IntegerProperty(choices=set(const.Video_States))    
    created_at  = db.DateTimeProperty(auto_now_add = True)
    
    
class UserVideos(db.Model):
    user        = db.ReferenceProperty(User)
    video       = db.ReferenceProperty(Video)
    state       = db.IntegerProperty(choices=set([const.User_Video_Favorited]))    
    created_at  = db.DateTimeProperty(auto_now_add = True)


class UserChannel(db.Model):
    user        = db.ReferenceProperty(User, required = True)
    channel     = db.ReferenceProperty(Channel, required = True)
    created_at  = db.DateTimeProperty(auto_now_add = True)    
    
class LastfmUser(db.Model):
    username    = db.StringProperty()
