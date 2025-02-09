import aioboto3
import asyncio
import os
import json
import shutil
import aiofiles
from random import randint
import time 

# Remplacez par vos credentials AWS (évitez de les hardcoder, utilisez des variables d'environnement ou IAM roles)
AWS_ACCESS_KEY = 'copypast'
AWS_SECRET_KEY = 'copypast'
AWS_REGION = 'eu-north-1'

BUCKET_NAME = f"deletebucket-{int(time.time())}"


class S3AsyncManager:
    """Gère les interactions avec AWS S3 en mode asynchrone."""
    
    def __init__(self, bucket_name, region):
        self.bucket_name = bucket_name
        self.region = region
        self.session = aioboto3.Session()

    async def create_bucket(self):
        """Crée un bucket S3."""
        async with self.session.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name=self.region) as s3:
            print(f">> Creating bucket named: {self.bucket_name}\n")
            try:
                response = await s3.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": self.region}
                )
                print(">>> Create Bucket Response:\n" + json.dumps(response, indent=4) + "\n")
            except Exception as e:
                print(f"Erreur lors de la création du bucket: {str(e)}")

    async def upload_files(self, folder_path):
        """Upload tous les fichiers d'un dossier vers S3."""
        async with self.session.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name=self.region) as s3:
            print(f"\n>> Copying new files to S3 bucket: {self.bucket_name}")
            tasks = []
            for filename in os.listdir(folder_path):
                file_path = os.path.join(folder_path, filename)
                print(f">>> Uploading: {filename}")
                task = s3.upload_file(Filename=file_path, Bucket=self.bucket_name, Key=filename)
                tasks.append(task)

            await asyncio.gather(*tasks)

    async def delete_bucket(self):
        """Supprime tous les fichiers du bucket puis le bucket lui-même."""
        async with self.session.resource("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name=self.region) as s3r:
            bucket = await s3r.Bucket(self.bucket_name)
            print("\n>>> Removing all objects from bucket:")
            
            async for obj in bucket.objects.all():
                print(f">>>>> Deleting: {obj.key}")
                await obj.delete()

            print("\n>>> Deleting bucket")
            async with self.session.client("s3") as s3:
                response = await s3.delete_bucket(Bucket=self.bucket_name)
                print(">>> Delete Bucket Response:\n" + json.dumps(response, indent=4) + "\n")


class FileManager:
    """Gère la création et la suppression des fichiers locaux."""
    
    def __init__(self, folder_path, video_source):
        self.folder_path = folder_path
        self.video_source = video_source
        os.makedirs(self.folder_path, exist_ok=True)

    def copy_videos(self, video_files):
        """Copie des fichiers vidéo vers le dossier local."""
        print(">> Copying video files to local folder.")
        for video in video_files:
            shutil.copyfile(os.path.join(self.video_source, video), os.path.join(self.folder_path, video))

    async def create_text_files(self, count=10):
        """Crée des fichiers texte avec du contenu aléatoire."""
        print(">> Creating text files.")
        for x in range(count):
            file_path = os.path.join(self.folder_path, f"file{x}.txt")
            print(f">>> Creating {file_path}")
            async with aiofiles.open(file_path, "w") as f:
                for i in range(randint(5, 100)):
                    await f.write(f"File {x} Line {i+1}\n")

    def cleanup(self):
        """Supprime les fichiers locaux."""
        print(">> Removing temporary files.")
        try:
            shutil.rmtree(self.folder_path)
        except OSError as e:
            print(f"Error: {e.filename} - {e.strerror}.")


class S3App:
    """Coordonne l'ensemble des opérations."""
    
    def __init__(self, bucket_name, region, folder_path, video_source):
        self.s3_manager = S3AsyncManager(bucket_name, region)
        self.file_manager = FileManager(folder_path, video_source)

    async def run(self):
        """Exécute l'application."""
        await self.s3_manager.create_bucket()
        self.file_manager.copy_videos(["movie-360.mp4", "movie-540.mp4", "movie-720.mp4"])
        await self.file_manager.create_text_files()

        await self.s3_manager.upload_files(self.file_manager.folder_path)

        print(f"\n>> You can now view the files in S3: https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/")
        delete = input(">> Would you like to delete the bucket? <y|n> ")

        if delete.lower() == "y":
            await self.s3_manager.delete_bucket()
        else:
            print("\n>>> Bucket kept. Remember, you are charged for storage!")

        self.file_manager.cleanup()


# Exécution principale
if __name__ == "__main__":
    app = S3App(BUCKET_NAME, AWS_REGION, "files", "/home/erwan/Videos/dash-test/dash")
    asyncio.run(app.run())
