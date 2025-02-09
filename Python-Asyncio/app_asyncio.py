import aioboto3
import asyncio
import os
import json
import shutil
import aiofiles
from random import randint
import time 

# Remplacez par vos credentials AWS (évitez de les hardcoder, utilisez des variables d'environnement ou IAM roles)
accessKey = 'copypast'
secretKey = 'copypast'
regionName = 'eu-north-1'
bucketName = f"deletebucket-{int(time.time())}"

async def create_bucket(session):
    async with session.client("s3", aws_access_key_id=accessKey, aws_secret_access_key=secretKey, region_name=regionName) as s3:
        print(f">> Creating bucket named: {bucketName}\n")
        try:
            response = await s3.create_bucket(
                Bucket=bucketName,
                CreateBucketConfiguration={"LocationConstraint": regionName}
            )
            print(">>> Create Bucket Response:\n" + json.dumps(response, indent=4) + "\n")
        except Exception as e:
            print(f"Erreur lors de la création du bucket: {str(e)}")

async def create_local_files():
    print(">> Creating local files to copy to bucket: ./files/")
    os.makedirs("files", exist_ok=True)

    # Copier des fichiers vidéo
    video_source = "/home/erwan/Videos/dash-test/dash/"
    video_files = ["movie-360.mp4", "movie-540.mp4", "movie-720.mp4"]
    
    for video in video_files:
        shutil.copyfile(os.path.join(video_source, video), os.path.join("files", video))

    # Créer des fichiers texte
    for x in range(10):
        file_path = f"files/file{x}.txt"
        print(f">>> Creating {file_path}")
        async with aiofiles.open(file_path, "w") as f:
            for i in range(randint(5, 100)):
                await f.write(f"File {x} Line {i+1}\n")

async def upload_files(session):
    async with session.client("s3", aws_access_key_id=accessKey, aws_secret_access_key=secretKey, region_name=regionName) as s3:
        print(f"\n>> Copying new files to S3 bucket: {bucketName}")
        tasks = []
        for filename in os.listdir("files"):
            file_path = os.path.join("files", filename)
            print(f">>> Uploading: {filename}")
            task = s3.upload_file(Filename=file_path, Bucket=bucketName, Key=filename)
            tasks.append(task)

        await asyncio.gather(*tasks)

async def delete_bucket(session):
    async with session.resource("s3", aws_access_key_id=accessKey, aws_secret_access_key=secretKey, region_name=regionName) as s3r:
        bucket = await s3r.Bucket(bucketName)
        print("\n>>> Removing all objects from bucket:")
        
        async for obj in bucket.objects.all():
            print(f">>>> Deleting: {obj.key}")
            await obj.delete()

        print("\n>>> Deleting bucket")
        async with session.client("s3") as s3:
            response = await s3.delete_bucket(Bucket=bucketName)
            print(">>> Delete Bucket Response:\n" + json.dumps(response, indent=4) + "\n")

async def main():
    session = aioboto3.Session()  

    await create_bucket(session)
    await create_local_files()
    await upload_files(session)

    await delete_bucket(session)

    """
    
    print(f"\n>> You can now view the files in S3: https://{bucketName}.s3.{regionName}.amazonaws.com/")
    delete = input(">> Would you like to delete the bucket? <y|n> ")
    
    if delete.lower() == "y":
        await delete_bucket(session)
    else:
        print("\n>>> Bucket kept. Remember, you are charged for storage!")

    # Nettoyage des fichiers temporaires
    print(">> Removing temporary files.")
    try:
        shutil.rmtree("files")
    except OSError as e:
        print(f"Error: {e.filename} - {e.strerror}.")
    """

# Exécuter le programme
asyncio.run(main())
