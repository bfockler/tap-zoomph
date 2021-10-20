#!/usr/bin/env python3

import argparse, datetime, json, sys, time
import backoff
import requests
import singer


endpoint = "https://api.zoomph.com/partnermention/report"
logger = singer.get_logger()

DATE_FORMAT="%Y-%m-%dT%H:%M:%S%Z"

def giveup(error):
    logger.error(error.response.text)
    response = error.response
    return not (response.status_code == 429 or
                response.status_code >= 500)


@backoff.on_exception(backoff.constant,
                      (requests.exceptions.RequestException),
                      jitter=backoff.random_jitter,
                      max_tries=5,
                      giveup=giveup,
                      interval=30)
def post(url, params, body):
    response = requests.post(url=url, params=params, data=body)
    response.raise_for_status()
    return response

@backoff.on_exception(backoff.constant,
                      (requests.exceptions.RequestException),
                      jitter=backoff.random_jitter,
                      max_tries=5,
                      giveup=giveup,
                      interval=30)
def get(url):
    response = requests.get(url=url)
    response.raise_for_status()
    return response


def do_sync(access_token, start_date, partners, feedId):
    # state = {"start_date": start_date}
    # next_date = start_date

    # if not end_date:
    end_date = (
        datetime.datetime.today() + datetime.timedelta(days=1)).strftime(DATE_FORMAT)

    params = {
        "access_token": access_token
    }

    body = {
        "Partners": partners,
        "FeedId": feedId,
        "Query": "created>"+start_date+" AND created<"+end_date
    }

    logger.info(json.dumps(params))

    try:
        response = post(endpoint, params, body)
        response = response.json()
        logger.info(json.dumps(response))
    except requests.exceptions.RequestException as e:
        logger.critical(
            json.dumps(
                {"url": e.request.url,
                 "status": e.response.status_code,
                 "message": e.response.text,
                 }
            )
        )
        singer.write_state(state)
        sys.exit(-1)

    reportID = response['ReportId']

    logger.info(json.dumps(params))

    url = endpoint + '/' + str(reportID) + '?access_token=' + access_token + '&fields=Id&fields=Partner&fields=PartnerMentionType&fields=ServiceType&fields=ServiceId&fields=PartnerExposureCreator&fields=PartnerExposureCreatorName&fields=PartnerExposureDate&fields=Message&fields=ContentType&fields=Url&fields=Verified&fields=FollowerCount&fields=FollewerInteractionRate&fields=Impressions&fields=ProjectedImpressions&fields=RetweetCount&fields=LikeCount&fields=ReplyCount&fields=CommentCount&fields=ViewCount&fields=LoveCount&fields=ShareCount&fields=WowCount&fields=HahaCount&fields=SadCount&fields=AngryCount&fields=AverageConcurrentViewers&fields=IsProrated&fields=AnalyzedProratedSeconds&fields=BrandExposureValue&fields=HoursWatched&fields=VideoTotalSeconds&fields=LogoAverageClarity&fields=LogoAverageSize&fields=LogoStartSeconds&fields=LogoEndSeconds&fields=LogoTotalSeconds&fields=LogoFrameCount&fields=LogoImpressions&fields=Mentions&fields=Hashtags&fields=Engagement&fields=FrameUrl&fields=Language&fields=ProvinceFips&fields=CountryFips&fields=Sentiment&fields=VideoViews&fields=Tags&fields=LogoAI&fields=TextMention&fields=PartnerAssetLabel&fields=AssetAI&fields=LogoOnAssetAI&fields=ExitsCount&fields=TapForwardCount&fields=TapBackwardCount&fields=DislikeCount&fields=Reach&fields=OrganicImpressions&fields=PaidImpressions&fields=EngagementRate&fields=FollowersGained&fields=PeakLiveViewerCount&fields=LiveViews&fields=ProjectedVideoViews&fields=PostValue&fields=Interactions&fields=FollowerInteractionRate&fields=LogoLocation&fields=AllLogos'
    logger.info(url)
    try:
        response = get(url)
        response = response.json()
        logger.info(json.dumps(response))
        logger.info(response['Report'])
        while response['Report'] is None:
          logger.info('Sleeping 1 minute to wait for report to generate')
          time.sleep(30)
          response = get(url)
          response = response.json()
    except requests.exceptions.RequestException as e:
        logger.critical(
            json.dumps(
                {"url": e.request.url,
                 "status": e.response.status_code,
                 "message": e.response.text,
                 }
            )
        )
        # singer.write_state(state)
        sys.exit(-1)

    schema = {
      "type": "object",
      "properties": {
        "AllLogos": {"type": "array"},
        "AnalyzedProratedSeconds": {"type": "number"},
        "AngryCount": {"type": "number"},
        "PartnerExposureCreatorName": {"type": "string"},
        "PartnerExposureCreator": {"type": "string"},
        "AverageConcurrentViewers": {"type": "number"},
        "BrandExposureValue": {"type": "number"},
        "CommentCount": {"type": "number"},
        "ContentType": {"type": "string"},
        "CountryFips": {"type": "string"},
        "PartnerExposureDate": {"type": "string", "format": "date-time"},
        "DislikeCount": {"type": "number"},
        "Engagement": {"type": "number"},
        "EngagementRate": {"type": "number"},
        "ExitsCount": {"type": "string"},
        "FollowerCount": {"type": "number"},
        "FollowersGained": {"type": "number"},
        "FollowerInteractionRate": {"type": "string"},
        "FrameUrl": {"type": "string"},
        "HahaCount": {"type": "number"},
        "Hashtags": {"type": "array"},
        "HoursWatched": {"type": "number"},
        "Id": {"type": "number", 'key': True},
        "Impressions": {"type": "number"},
        "Interactions": {"type": "number"},
        "IsProrated": {"type": "boolean"},
        "Language": {"type": "string"},
        "LikeCount": {"type": "number"},
        "LiveViews": {"type": "number"},
        "LogoAverageClarity": {"type": "number"},
        "LogoAverageSize": {"type": "string"},
        "LogoEndSeconds": {"type": "number"},
        "LogoFrameCount": {"type": "number"},
        "LogoImpressions": {"type": "number"},
        "LogoLocation": {"type": "string"},
        "LogoStartSeconds": {"type": "number"},
        "LogoTotalSeconds": {"type": "number"},
        "LoveCount": {"type": "number"},
        "Mentions": {"type": "array"},
        "Message": {"type": "string"},
        "Logo AI": {"type": "string"},
        "Text Mention": {"type": "string"},
        "Tags": {"type": "string"},
        "OrganicImpressions": {"type": "number"},
        "PaidImpressions": {"type": "number"},
        "Partner": {"type": "string"},
        "PartnerAssetLabel": {"type": "string"},
        "PartnerMentionType": {"type": "string"},
        "PeakLiveViewerCount": {"type": "number"},
        "PostValue": {"type": "number"},
        "ProjectedImpressions": {"type": "number"},
        "ProjectedVideoViews": {"type": "number"},
        "ProvinceFips": {"type": "string"},
        "Reach": {"type": "number"},
        "ReplyCount": {"type": "number"},
        "RetweetCount": {"type": "number"},
        "SadCount": {"type": "number"},
        "Sentiment": {"type": "string"},
        "ServiceId": {"type": "number"},
        "ServiceType": {"type": "string"},
        "ShareCount": {"type": "number"},
        "Url": {"type": "string"},
        "Verified": {"type": "boolean"},
        "VideoTotalSeconds": {"type": "number"},
        "VideoViews": {"type": "number"},
        "ViewCount": {"type": "number"},
        "VODViews": {"type": "number"},
        "WowCount": {"type": "number"},
      },
    }

    singer.write_schema("zoomph", schema, "Id")

    posts = response['Report']

    for p in posts:
      singer.write_records("zoomph", [p])


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-c", "--config", help="Config file", required=False)
    parser.add_argument(
        "-s", "--state", help="State file", required=False)

    args = parser.parse_args()

    if args.config:
        with open(args.config) as f:
            config = json.load(f)
    else:
        config = {}

    if args.state:
        with open(args.state) as f:
            state = json.load(f)
    else:
        state = {}

    start_date = (config.get("start_date") or
                  datetime.datetime.utcnow().strftime(DATE_FORMAT))
    start_date = singer.utils.strptime_with_tz(
        start_date).date().strftime(DATE_FORMAT)

    end_date = (config.get("end_date") or
                datetime.datetime.utcnow().strftime(DATE_FORMAT))
    end_date = singer.utils.strptime_with_tz(
        end_date).date().strftime(DATE_FORMAT)

    do_sync(config.get("access_token"), start_date, config.get("partners"), config.get("feed_id"))


if __name__ == "__main__":
    main()