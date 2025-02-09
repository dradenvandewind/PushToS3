package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// "eu-north-1"
const (
	regionName  = "us-east-1"
	videoSource = "/home/erwan/Videos/dash-test/dash"
	filesFolder = "files"
)

var (
	bucketName = fmt.Sprintf("deletebucket-%d", time.Now().Unix())
)

// S3Manager gère les interactions avec AWS S3
type S3Manager struct {
	Client *s3.Client
}

// NewS3Manager initialise un gestionnaire S3
func NewS3Manager() *S3Manager {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(regionName))
	if err != nil {
		log.Fatalf("Erreur lors du chargement de la configuration AWS: %v", err)
	}

	client := s3.NewFromConfig(cfg)
	return &S3Manager{
		Client: client,
	}
}

// CreateBucket crée un bucket S3
func (s *S3Manager) CreateBucket() error {
	fmt.Printf(">> Creating bucket named: %s\n", bucketName)

	_, err := s.Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})

	if err != nil {
		return fmt.Errorf("erreur lors de la création du bucket: %v", err)
	}

	fmt.Println(">>> Bucket créé avec succès")
	return nil
}

// UploadFiles envoie les fichiers locaux vers S3
func (s *S3Manager) UploadFiles(folderPath string) error {
	fmt.Printf("\n>> Uploading files to S3 bucket: %s\n", bucketName)

	files, err := os.ReadDir(folderPath)
	if err != nil {
		return fmt.Errorf("erreur lors de la lecture des fichiers: %v", err)
	}

	var wg sync.WaitGroup
	for _, file := range files {
		wg.Add(1)
		go func(filename string) {
			defer wg.Done()

			filePath := filepath.Join(folderPath, filename)
			fileContent, err := os.Open(filePath)
			if err != nil {
				log.Printf("Erreur d'ouverture de fichier %s: %v", filename, err)
				return
			}
			defer fileContent.Close()

			_, err = s.Client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(filename),
				Body:   fileContent,
			})
			if err != nil {
				log.Printf("Erreur d'upload de %s: %v", filename, err)
			} else {
				fmt.Printf(">>> Upload réussi: %s\n", filename)
			}
		}(file.Name())
	}
	wg.Wait()
	return nil
}

// DeleteBucket supprime les fichiers et le bucket S3
func (s *S3Manager) DeleteBucket() error {
	fmt.Println("\n>>> Suppression des fichiers du bucket")

	objects, err := s.Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return fmt.Errorf("erreur de listing des objets: %v", err)
	}

	for _, obj := range objects.Contents {
		_, err := s.Client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    obj.Key,
		})
		if err != nil {
			log.Printf("Erreur suppression de %s: %v", *obj.Key, err)
		} else {
			fmt.Printf(">>> Fichier supprimé: %s\n", *obj.Key)
		}
	}

	fmt.Println("\n>>> Suppression du bucket")
	_, err = s.Client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return fmt.Errorf("erreur lors de la suppression du bucket: %v", err)
	}
	fmt.Println(">>> Bucket supprimé avec succès")
	return nil
}

// FileManager gère les fichiers locaux
type FileManager struct {
	FolderPath  string
	VideoSource string
}

// NewFileManager initialise un gestionnaire de fichiers
func NewFileManager(folderPath, videoSource string) *FileManager {
	os.MkdirAll(folderPath, os.ModePerm)
	return &FileManager{FolderPath: folderPath, VideoSource: videoSource}
}

// CopyVideos copie les fichiers vidéos
func (f *FileManager) CopyVideos(videoFiles []string) {
	fmt.Println(">> Copying video files to local folder.")
	for _, video := range videoFiles {
		src := filepath.Join(f.VideoSource, video)
		dst := filepath.Join(f.FolderPath, video)
		input, err := os.ReadFile(src)
		if err != nil {
			log.Printf("Erreur lors de la copie de %s: %v", video, err)
			continue
		}
		err = os.WriteFile(dst, input, 0644)
		if err != nil {
			log.Printf("Erreur d'écriture de %s: %v", video, err)
		}
	}
}

// CreateTextFiles crée des fichiers textes avec du contenu aléatoire
func (f *FileManager) CreateTextFiles(count int) {
	fmt.Println(">> Creating text files.")
	for i := 0; i < count; i++ {
		filePath := filepath.Join(f.FolderPath, fmt.Sprintf("file%d.txt", i))
		content := fmt.Sprintf("File %d Content\n", i)
		err := os.WriteFile(filePath, []byte(content), 0644)
		if err != nil {
			log.Printf("Erreur de création de fichier %s: %v", filePath, err)
		}
	}
}

// Cleanup supprime le dossier temporaire
func (f *FileManager) Cleanup() {
	fmt.Println(">> Suppression des fichiers temporaires.")
	err := os.RemoveAll(f.FolderPath)
	if err != nil {
		log.Printf("Erreur lors de la suppression des fichiers temporaires: %v", err)
	}
}

// S3App gère l'exécution de l'application
type S3App struct {
	S3Manager   *S3Manager
	FileManager *FileManager
}

// NewS3App initialise l'application
func NewS3App() *S3App {
	return &S3App{
		S3Manager:   NewS3Manager(),
		FileManager: NewFileManager(filesFolder, videoSource),
	}
}

// Run exécute l'application
func (app *S3App) Run() {
	err := app.S3Manager.CreateBucket()
	if err != nil {
		log.Fatalf("Erreur création bucket: %v", err)
	}

	app.FileManager.CopyVideos([]string{"movie-360.mp4", "movie-540.mp4", "movie-720.mp4"})
	app.FileManager.CreateTextFiles(10)

	err = app.S3Manager.UploadFiles(app.FileManager.FolderPath)
	if err != nil {
		log.Fatalf("Erreur upload fichiers: %v", err)
	}
	/*
		fmt.Printf("\n>> You can now view the files in S3: https://%s.s3.%s.amazonaws.com/\n", bucketName, regionName)
		fmt.Print(">> Would you like to delete the bucket? <y|n> ")
		var response string
		fmt.Scanln(&response)

		if strings.ToLower(response) == "y" {
			err = app.S3Manager.DeleteBucket()
			if err !=  nil{
				log.Fatalf("Erreur suppression bucket: %v", err)
			}
		} else {
			fmt.Println(">>> Bucket conservé.")
		}

	*/
	err = app.S3Manager.DeleteBucket()
	if err != nil {
		log.Fatalf("Erreur suppression bucket: %v", err)
	}

	app.FileManager.Cleanup()
}

func main() {
	app := NewS3App()
	app.Run()
}
