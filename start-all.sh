#!/bin/sh

nohup sh get_news_url.sh &> getUrl.log &
nohup sh get_news_content.sh &> getContent.log &
nohup sh get_news_view.sh &> getView.log &
