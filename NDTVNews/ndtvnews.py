# -*- coding: utf-8 -*-

import feedparser
gnews = feedparser.parse("http://feeds.feedburner.com/NDTV-Trending?format=xml")
print gnews['feed']['title']
for i in range (0,len(gnews['entries'])):
	print "News Type : %s || Title: %s"%(gnews['entries'][i]['category'],gnews['entries'][i]['title'])
	#print gnews['entries'][i]['link']