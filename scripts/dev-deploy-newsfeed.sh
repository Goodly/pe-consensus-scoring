#!/bin/bash
aws s3 cp ./newsfeed/newsfeed.html s3://dev.publiceditor.io/newsfeed/ --acl public-read
aws s3 cp ./newsfeed/menu.html s3://dev.publiceditor.io/newsfeed/ --acl public-read
aws s3 cp --recursive ./newsfeed/assets/ s3://dev.publiceditor.io/newsfeed/assets/ --acl public-read
