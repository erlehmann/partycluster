#!/usr/bin/env python
# -*- coding: utf-8 -*-

# partycluster.py – finds parties
# Copyright (C) 2012  Nils Dagsson Moskopp

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from sys import argv, stderr, stdout

from werkzeug.contrib.cache import FileSystemCache
from progressbar import ProgressBar
from requests import get
from cStringIO import StringIO
from xml.etree.cElementTree import dump, ElementTree
from iso8601 import parse_date
from geopy import Point, distance
from cluster import HierarchicalClustering

feed_cache = FileSystemCache('cache_dir', 3600)

class Event():
    def __init__(self, name, uri, datetime, latitude, longitude):
        self.name = name
        self.uri = uri
        self.datetime = datetime
        self.latitude = latitude
        self.longitude = longitude

    def __repr__(self):
        return "%s <%s>, %s @ %s, %s" % (self.name, self.uri, self.datetime, self.latitude, self.longitude)

def eventDistance(a, b):
    """
    Calculates the spatial distance between two events.
    """
    point_a = Point(a.latitude, a.longitude)
    point_b = Point(b.latitude, b.longitude)
    return distance.distance(point_a, point_b).m

def maximumEventDistance(events):
    """
    Calculates the maximum spatial distance between many events.
    """
    maximum_distance = 0
    for event_a in events:
        for event_b in events:
            if event_a == event_b:
                continue
            distance = eventDistance(event_a, event_b)
            if distance > maximum_distance:
                maximum_distance = distance
    return maximum_distance
        

def getEvents(feed):
    """
    Creates events from an ATOM feed with GeoRSS points.
    """
    events = []

    tree = ElementTree()
    tree.parse(feed)

    entries = ElementTree(tree).iter('{http://www.w3.org/2005/Atom}entry')
    for entry in entries:
        author = entry.find('{http://www.w3.org/2005/Atom}author')
        try:
            name = author.find('{http://www.w3.org/2005/Atom}name').text
            uri = author.find('{http://www.w3.org/2005/Atom}uri').text
        except AttributeError:
            continue

        try:
            point = entry.find('{http://www.georss.org/georss}point').text
            latitude = point.split()[0]
            longitude = point.split()[1]
        except AttributeError:
            continue

        published = parse_date(
            entry.find('{http://www.w3.org/2005/Atom}published').text
        )
        event = Event(name, uri, published, latitude, longitude)
        events.append(event)

    return events

def updateEvents(current_events, new_events):
    """
    Updates a given list of events, substituting newer events.
    """
    for new_event in new_events:
        key = new_event.name + ' <' + new_event.uri + '>'
        try:
            current_event = current_events[key]
            if new_event.datetime > current_event.datetime:
                current_events[key] = new_event
        except KeyError:
            current_events[key] = new_event
    return current_events

def getPlaceName(latitude, longitude):
    url = "http://ws.geonames.org/findNearbyPlaceName?lat=%s&lng=%s" % \
        (latitude, longitude)
    request = get(url)
    tree = ElementTree()
    tree.parse(StringIO(request.text.encode('utf-8')))
    return tree.find('geoname/toponymName').text

def partyPrint(cluster, threshold):
    """
    Prints a party announcement!
    """
    names = [c.name for c in cluster]
    timestamps = [c.datetime.strftime('%H:%M') for c in cluster]
    placeNames = [getPlaceName(c.latitude, c.longitude) for c in cluster]
    stdout.write('Verdacht auf Party mit %s um Umkreis von %s Metern um %s ' % (
        ', '.join(names[:-1]) + ' und ' + names[-1],
        int(maximumEventDistance(cluster)),
        ', '.join(placeNames[:-1]) + ' und ' + placeNames[-1]
    ))
    stdout.write('(%s).\n' % ', '.join(timestamps))


try:
    filename = argv[1]
    threshold = int(argv[2])
except IndexError:
    stderr.write("""Nutzung: partycluster.py [Feedliste] [Grenzwert]

Feedliste ist eine Datei mit einem URL zu einem ATOM-Feed pro Zeile.
Grenzwert ist die maximale Entfernung von Partyteilnehmern.
""")
    exit(1)

current_events = {}
with open(filename, 'r') as feeds:
    progress = ProgressBar(maxval=len(feeds.readlines()))

with open(filename, 'r') as feeds:
    for line in feeds:
        feed_url = line.strip()
        cached_content = feed_cache.get(feed_url)
        if not cached_content:
            request = get(feed_url)
            events = getEvents(StringIO(request.text.encode('utf-8')))
            feed_cache.set(feed_url, request.text.encode('utf-8'))
        else:
            events = getEvents(StringIO(cached_content))
        current_events = updateEvents(current_events, events)
        progress.update(progress.currval+1)

clustering = HierarchicalClustering(current_events.values(), eventDistance)
clusters = clustering.getlevel(threshold)

for cluster in clusters:
    if len(cluster) > 2:
        partyPrint(cluster, threshold)
