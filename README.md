# Twitter Bot
This is a Twitter/X bot that uploads image from Google Photos album to Twitter/X with added text. It's supposed to be run on AWS Lambda, it stores history of uploads in DynamoDB database so posts from album wont get repeated in short time frame, it also stores refresh tokens for Google Photos API there. For photos above 5MB needed compression is applied.
