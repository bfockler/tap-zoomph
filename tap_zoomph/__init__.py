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

    url = endpoint + '/' + str(reportID) + '?access_token=' + access_token + '&fields=Id&fields=Partner&fields=PartnerMentionType&fields=ServiceType&fields=ServiceId&fields=PartnerExposureCreator&fields=PartnerExposureCreatorName&fields=PartnerExposureDate&fields=Message&fields=ContentType&fields=Url&fields=Verified&fields=FollowerCount&fields=FollewerInteractionRate&fields=Impressions&fields=ProjectedImpressions&fields=RetweetCount&fields=LikeCount&fields=ReplyCount&fields=CommentCount&fields=ViewCount&fields=LoveCount&fields=ShareCount&fields=WowCount&fields=HahaCount&fields=SadCount&fields=AngryCount&fields=AverageConcurrentViewers&fields=IsProrated&fields=AnalyzedProratedSeconds&fields=BrandExposureValue&fields=HoursWatched&fields=VideoTotalSeconds&fields=LogoAverageClarity&fields=LogoAverageSize&fields=LogoStartSeconds&fields=LogoEndSeconds&fields=LogoTotalSeconds&fields=LogoFrameCount&fields=LogoImpressions&fields=Mentions&fields=Hashtags&fields=Engagement&fields=FrameUrl&fields=Language&fields=ProvinceFips&fields=CountryFips&fields=Sentiment&fields=VideoViews&fields=TextMention&fields=AssetAI&fields=LogoOnAssetAI&fields=ExitsCount&fields=TapForwardCount&fields=TapBackwardCount&fields=DislikeCount&fields=Reach&fields=OrganicImpressions&fields=PaidImpressions&fields=EngagementRate&fields=FollowersGained&fields=PeakLiveViewerCount&fields=LiveViews&fields=ProjectedVideoViews&fields=PostValue&fields=Interactions&fields=FollowerInteractionRate&fields=LogoLocation&fields=AllLogos'
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
        "AllLogos": {"type": ["array", "null"]},
        "AnalyzedProratedSeconds": {"type": ["number", "null"]},
        "AngryCount": {"type": ["number", "null"]},
        "PartnerExposureCreatorName": {"type": ["string", "null"]},
        "PartnerExposureCreator": {"type": ["string", "null"]},
        "AverageConcurrentViewers": {"type": ["number", "null"]},
        "BrandExposureValue": {"type": ["number", "null"]},
        "CommentCount": {"type": ["number", "null"]},
        "ContentType": {"type": ["string", "null"]},
        "CountryFips": {"type": ["string", "null"]},
        "PartnerExposureDate": {"type": ["string", "null"], "format": "date-time"},
        "DislikeCount": {"type": ["number", "null"]},
        "Engagement": {"type": ["number", "null"]},
        "EngagementRate": {"type": ["number", "null"]},
        "ExitsCount": {"type": ["number", "null"]},
        "FollowerCount": {"type": ["number", "null"]},
        "FollowersGained": {"type": ["number", "null"]},
        "FollowerInteractionRate": {"type": ["number", "null"]},
        "FrameUrl": {"type": ["string", "null"]},
        "HahaCount": {"type": ["number", "null"]},
        "Hashtags": {"type": ["array", "null"]},
        "HoursWatched": {"type": ["number", "null"]},
        "Id": {"type": "number", 'key': True},
        "Impressions": {"type": ["number", "null"]},
        "Interactions": {"type": ["number", "null"]},
        "IsProrated": {"type": ["boolean", "null"]},
        "Language": {"type": ["string", "null"]},
        "LikeCount": {"type": ["number", "null"]},
        "LiveViews": {"type": ["number", "null"]},
        "LogoAverageClarity": {"type": ["number", "null"]},
        "LogoAverageSize": {"type": ["string", "null"]},
        "LogoEndSeconds": {"type": ["number", "null"]},
        "LogoFrameCount": {"type": ["number", "null"]},
        "LogoImpressions": {"type": ["number", "null"]},
        "LogoLocation": {"type": ["string", "null"]},
        "LogoStartSeconds": {"type": ["number", "null"]},
        "LogoTotalSeconds": {"type": ["number", "null"]},
        "LoveCount": {"type": ["number", "null"]},
        "Mentions": {"type": ["array", "null"]},
        "Message": {"type": ["string", "null"]},
        "Text Mention": {"type": ["array", "string", "null"]},
        "OrganicImpressions": {"type": ["number", "null"]},
        "PaidImpressions": {"type": ["number", "null"]},
        "Partner": {"type": ["string", "null"]},
        "PartnerMentionType": {"type": ["string", "null"]},
        "PeakLiveViewerCount": {"type": ["number", "null"]},
        "PostValue": {"type": ["number", "null"]},
        "ProjectedImpressions": {"type": ["number", "null"]},
        "ProjectedVideoViews": {"type": ["number", "null"]},
        "ProvinceFips": {"type": ["string", "null"]},
        "Reach": {"type": ["number", "null"]},
        "ReplyCount": {"type": ["number", "null"]},
        "RetweetCount": {"type": ["number", "null"]},
        "SadCount": {"type": ["number", "null"]},
        "Sentiment": {"type": ["string", "null"]},
        "ServiceId": {"type": ["string", "null"]},
        "ServiceType": {"type": ["string", "null"]},
        "ShareCount": {"type": ["number", "null"]},
        "Url": {"type": ["string", "null"]},
        "Verified": {"type": ["boolean", "null"]},
        "VideoTotalSeconds": {"type": ["number", "null"]},
        "VideoViews": {"type": ["number", "null"]},
        "ViewCount": {"type": ["number", "null"]},
        "VODViews": {"type": ["number", "null"]},
        "WowCount": {"type": ["number", "null"]},
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