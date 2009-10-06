require 'rubygems'
require 'hpricot'
require 'open-uri'

links = []
links << "http://en.wikipedia.org/wiki/List_of_record_labels"
links << "http://en.wikipedia.org/wiki/List_of_record_labels:_A-H"
links << "http://en.wikipedia.org/wiki/List_of_record_labels:_I-Q"
links << "http://en.wikipedia.org/wiki/List_of_record_labels:_R-Z"

links.each do |label_link|
  doc = Hpricot.parse(open(label_link))
  (doc/"div#bodyContent//table.multicol//a").each do |link|
    p link.innerHTML
  end
end

