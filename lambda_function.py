import os, os.path
import pickle
from google_auth_oauthlib.flow import Flow, InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2 import credentials
import random
import numpy as np
import requests
import tweepy
from tweepy.auth import OAuthHandler
import json
import boto3
import tempfile
import codecs
import ast
import re
import tenacity
from tenacity import retry
from tenacity import stop_after_attempt
from tenacity import wait_exponential
from PIL import Image
import io
import API_KEYS

dynamodb = boto3.resource('dynamodb')
#input name of the dynamodb table
table = dynamodb.Table('DynamoDBTable')
#input name of the dynamodb key
dynamoDBKey = 'dynamoDBKey'
#input number of the dynamodb key column
uploadHistoryKey = 9998
numberOfPhotos = 4930
radiusRange = 3
#input google photos album name
gPhotoAlbumName = 'Google Album'

def listToString(s): 
    stringToReturn = "["
    for x in s:
        stringToReturn = stringToReturn + str(x) + ", "
    stringToReturn = stringToReturn[:-2] + "]"
    return stringToReturn

def compressPhoto(pathToFile, org_string):
    file_size = os.path.getsize(pathToFile) / 1000000
    if(file_size < 5):
        return pathToFile
    numberOfColors = 256
    finalImgPath = pathToFile
    img = Image.open(finalImgPath)
    quality=100
    l_posts_to_jpeg = json.loads(API_KEYS.L_POSTS_TO_JPEG)
    
    if(img.mode == 'L' and int(org_string[:6]) not in l_posts_to_jpeg):
        while file_size > 5:
            img_compressed = img.convert("P", palette=Image.ADAPTIVE, dither=Image.FLOYDSTEINBERG, colors=int(numberOfColors))
            #print("Current number of colors: " + str(numberOfColors))
            
            image_bytes = io.BytesIO()
            img_compressed.save(image_bytes, format='PNG')
            
            size_in_bytes = image_bytes.getbuffer().nbytes
            file_size = size_in_bytes / 1000000
            #print("Current file size: " + str(file_size))
            
            if(file_size > 5):
                numberOfColors = numberOfColors / 2
            else:
                #print("Final number of colors: " + str(numberOfColors))
                fd, path = tempfile.mkstemp(suffix='.png') 
                file = open(path, "wb")
                img_compressed.save(file, format='PNG', optimize=True)
                file.close()
                finalImgPath = path
                break
    else: 
        while file_size > 5:
            img_compressed = img.convert("RGB")
            #print("Current quality: " + str(quality))
            
            image_bytes = io.BytesIO()
            img_compressed.save(image_bytes, format='JPEG', quality=quality)
            
            size_in_bytes = image_bytes.getbuffer().nbytes
            file_size = size_in_bytes / 1000000
            #print("Current file size: " + str(file_size))
            
            if(file_size > 5):
                quality = quality - 2
            else:
                #print("Final quality: " + str(quality))
                fd, path = tempfile.mkstemp(suffix='.jpeg')
                file = open(path, "wb")
                img_compressed.save(file, format='JPEG', quality=quality)
                file.close()
                finalImgPath = path
                break
    
    return finalImgPath

def getEpString(postName):
    s = re.search("S", postName)
    episodeCodeS = postName[s.start()+1:s.start()+3]

    ed = re.search("ED", postName)
    e = re.search("E", postName)
    op = re.search("OP", postName)
    if ed is not None:
        epId = " Ending " + str(int(postName[ed.start()+2:ed.start()+4]))
    elif e is not None:
        epId = " Episode " + str(int(postName[e.start()+1:e.start()+3]))
    elif op is not None:
        epId = " Opening " + str(int(postName[op.start()+2:op.start()+4]))
    else:
        epId = " error"

    episodeString = "Season " + str(int(episodeCodeS)) + epId
    
    return episodeString

@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=2, min=1, max=7))
def postToAWSdb(stringToPost, gbKey):
    resp = table.update_item(
        Key={ dynamoDBKey : gbKey},
        #uploadHistoryString is the name of the column in dynamodb table
        UpdateExpression="SET uploadHistoryString= :s",
        ExpressionAttributeValues={':s': stringToPost},
        ReturnValues="UPDATED_NEW"
    )

@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=2, min=1, max=7))
def getFromAWSdb(gbKey):
    response = table.get_item(
        Key={
            dynamoDBKey : gbKey
        },
        ConsistentRead=True
    )
    return response

#returns new random number, this new number wasnt used in previous 1080 iterations and it's not in the radius of 3 in numbers chosen in last 360 iterations
def getPostNumber():
    uploadHistory = []
    uploadHistory.clear()
    uploadHistoryString = ""
    
    try:
        response = getFromAWSdb(uploadHistoryKey)
    except Exception as e:
        print(e)
    uploadHistoryString = str(response["Item"]["uploadHistoryString"])
    uploadHistory = json.loads(uploadHistoryString)
    
    randomNumber = random.randint(1, numberOfPhotos)
    isInRadius = []
    for x in range(0,360):
        isInRadius.append(False)
    
    while True:
        if randomNumber in uploadHistory:
            notYetNew = True
        else:
            notYetNew = False
        
        contentId = 1079
        isInRadiusId = 0
        
        for x in range(0,len(isInRadius)):
            if randomNumber >= (uploadHistory[contentId]-radiusRange) and randomNumber <= (uploadHistory[contentId]+radiusRange):
                isInRadius[isInRadiusId] = True
            else:
                isInRadius[isInRadiusId] = False
            contentId = contentId - 1
            isInRadiusId = isInRadiusId + 1
        
        if len(np.unique(isInRadius)) == 1 and isInRadius[0] == False:
            isNotInRadius = True
        else:
            isNotInRadius = False
        
        if notYetNew == False and isNotInRadius:
            uploadHistory.pop(0)
            uploadHistory.append(randomNumber)
            break
        else:
            randomNumber = random.randint(1, numberOfPhotos)
            continue
        
    uploadHistoryStringNew = ""
    uploadHistoryStringNew = listToString(uploadHistory)
    try:
        postToAWSdb(uploadHistoryStringNew, uploadHistoryKey)
    except Exception as e:
        print(e)
    #print(resp['Attributes'])
    
    return randomNumber
    
def resetDB():
    dbResetList = []
    for x in range(1, 1081):
        dbResetList.append(x)
    dbResetString = listToString(dbResetList)
    resp = table.update_item(
        Key={ dynamoDBKey : 9998},
        UpdateExpression="SET uploadHistoryString= :s",
        ExpressionAttributeValues={':s': dbResetString},
        ReturnValues="UPDATED_NEW"
    )

def Create_Service(client_secret_file, api_name, api_version, *scopes):
    CLIENT_SECRET_FILE = client_secret_file
    API_SERVICE_NAME = api_name
    API_VERSION = api_version
    SCOPES = [scope for scope in scopes[0]]

    cred = None
    grandBlueKey = 9999
        
    try:
        response = getFromAWSdb(grandBlueKey)
        
        fd, tempJsonCred = tempfile.mkstemp(suffix='.json')
        stringJsonCred = str(response["Item"]["PickleFile"])
        dictJsonCred = ast.literal_eval(stringJsonCred)
            
        with open(tempJsonCred, 'w') as token:
            json.dump(dictJsonCred, token)
        cred = credentials.Credentials.from_authorized_user_file(tempJsonCred)
    except Exception as e:
        print(e)

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            cred = flow.run_local_server()

        credToDB = cred.to_json()
        try:
            resp = table.update_item(
                Key={'GrandBlueKey': grandBlueKey},
                #PickleFile is name of the dynamodb column
                UpdateExpression="SET PickleFile= :s",
                ExpressionAttributeValues={':s': credToDB},
                ReturnValues="UPDATED_NEW"
            )
        except Exception as e:
            print(e)
    try:
        service = build(API_SERVICE_NAME, API_VERSION, credentials=cred, static_discovery=False)
        return service
    except Exception as e:
        print(e)
    return None

@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=3, min=3, max=21))
def getMediaListGPhotos(service, albumName):
    response_albums_list = service.albums().list().execute()
    albums_list = response_albums_list.get('albums')
    album_id = next(filter(lambda x: albumName in x['title'], albums_list))['id']

    request_body = {
        'albumId': album_id,
        'pageSize': 100
    }
    
    response_search = service.mediaItems().search(body=request_body).execute()
    
    lstMediaItems = response_search.get('mediaItems')
    nextPageToken = response_search.get('nextPageToken')
    
    while nextPageToken:
        request_body['pageToken'] = nextPageToken
    
        response_search = service.mediaItems().search(body=request_body).execute()
        lstMediaItems.extend(response_search.get('mediaItems'))
        nextPageToken = response_search.get('nextPageToken')
    
    return lstMediaItems

@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=3, min=3, max=21))
def downloadPhotoGPhotos(postUrl):
    response = requests.get(postUrl + "=d")
    return response

def saveGPhoto(response):
    fd, path = tempfile.mkstemp(suffix='.png') 
    file = open(path, "wb")
    file.write(response.content)
    file.close()
    return path

def getPhoto(postToPost):
    albumName = gPhotoAlbumName
    API_NAME = 'photoslibrary'
    API_VERSION = 'v1'
    CLIENT_SECRET_FILE = 'client_secret_Grand_Blue_Manga.json'
    SCOPES = ['https://www.googleapis.com/auth/photoslibrary',
              'https://www.googleapis.com/auth/photoslibrary.sharing']
    
    service = Create_Service(CLIENT_SECRET_FILE,API_NAME, API_VERSION, SCOPES)
    if service is None:
        return None, None

    try:
        lstMediaItems = getMediaListGPhotos(service, albumName)
    except tenacity.RetryError as e:
        print("Getting Media List")
        print("We caught the error: {}".format(e))
    
    for x in range(0, len(lstMediaItems)):
        org_string = lstMediaItems[x]['filename']
        mod_string = org_string[:6]
        if postToPost == int(mod_string):
            postUrl = lstMediaItems[x]['baseUrl']
            break
    try:
        response = downloadPhotoGPhotos(postUrl)
    except tenacity.RetryError as e:
        print("Downloading GPhoto")
        print("We caught the error: {}".format(e))
        
    path = saveGPhoto(response)
        
    try:
        path = compressPhoto(path, org_string)
    except Exception as e:
        print("Compress Photo")
        print(e)
    return path, org_string

@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=3, min=3, max=21))
def uploadMediaToTwitter(clientV1, pathToImage, fileType, altText):
    media = clientV1.chunked_upload(filename=pathToImage, file_type=fileType, wait_for_async_finalize=True)
    clientV1.create_media_metadata(media_id=media.media_id, alt_text=altText)
    return media

@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=3, min=3, max=21))
def uploadTweetToTwitter(clientV2, tweetText, media):
    clientV2.create_tweet(text=tweetText, media_ids=[media.media_id])
    
def uploadPost(pathToImage, postString):
    #postString is for season and episode numbers
    tweetText = "Text you want to post together with the photo" + postString
    
    fileExtensionPattern = r"\.([^.]+)$"
    matchedExtension = re.search(fileExtensionPattern, pathToImage)
    if matchedExtension.group(1) is not None:
        fileType = "image/" + matchedExtension.group(1)
    else:
        fileType = "image/png"
    
    try:
        auth = tweepy.OAuth1UserHandler(API_KEYS.CONSUMER_KEY, API_KEYS.CONSUMER_SECRET)
        auth.set_access_token(API_KEYS.ACCESS_TOKEN, API_KEYS.ACCESS_TOKEN_SECRET)
        clientV1 = tweepy.API(auth)
    except Exception as e:
        print("Client V1")
        print(e)
    
    try:
        media = uploadMediaToTwitter(clientV1, pathToImage, fileType, postString)
    except tenacity.RetryError as e:
        print("Media Upload")
        print("We caught the error: {}".format(e))
    
    try:
        clientV2 = tweepy.Client(
            bearer_token = API_KEYS.BEARER_TOKEN,
            consumer_key = API_KEYS.CONSUMER_KEY,
            consumer_secret = API_KEYS.CONSUMER_SECRET,
            access_token = API_KEYS.ACCESS_TOKEN,
            access_token_secret = API_KEYS.ACCESS_TOKEN_SECRET)
    except Exception as e:
        print("Client V2")
        print(e)
        
    try:
        uploadTweetToTwitter(clientV2, tweetText, media)
    except tenacity.RetryError as e:
        print("Tweet Create")
        print("We caught the error: {}".format(e))
    
def lambda_handler(event, context):
    #resetDB()
    postNumber, wasLastPostAnime = getPostNumber()
    pathToImage, postName = getPhoto(postNumber)
    postString = getEpString(postName)
    uploadPost(pathToImage, postString)
    
    return{
        'status_code': 200,
        'body': json.dumps('Success')
    }