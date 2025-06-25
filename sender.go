package main

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/ini.v1"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	totalFilesSent   int
	totalBytesSent   int64
	lastFileSentName string
	lastFileSentTime time.Time
	fileFirstSeen    = make(map[string]time.Time)
	fileMutex        sync.Mutex

	// Конфигурационные переменные
	serverAddr string
	username   string
	password   string
	sendDir    string
	archiveDir string
	logDir     string
	logFile    string
	useHTTPS   bool
	certFile   string
	keyFile    string
	numWorkers int
)

func init() {
	createConfigIfNotExists()
	updateConfigIfNeeded()

	var err error
	cfg, err := ini.Load("config.ini")
	if err != nil {
		log.Error().Msg(fmt.Sprintf("error loading the config.ini file: %s", err))
	}

	useHTTPS, _ = cfg.Section("Server").Key("UseHTTPS").Bool()
	protocol := "http"
	if useHTTPS {
		protocol = "https"
		certFile = cfg.Section("Server").Key("CertFile").String()
		keyFile = cfg.Section("Server").Key("KeyFile").String()
	}

	//omitting values that not used
	_ = certFile
	_ = keyFile

	serverAddr = fmt.Sprintf("%s://%s:%s/%s",
		protocol,
		cfg.Section("Server").Key("Host").String(),
		cfg.Section("Server").Key("Port").String(),
		cfg.Section("Server").Key("Context").String())

	username = cfg.Section("Auth").Key("Username").String()
	password = cfg.Section("Auth").Key("Password").String()

	sendDir = cfg.Section("Directories").Key("SendDir").String()
	archiveDir = cfg.Section("Directories").Key("ArchiveDir").String()
	logDir = cfg.Section("Directories").Key("LogDir").String()

	logFile = cfg.Section("File").Key("LogFile").String()
	numWorkers, _ = cfg.Section("Goroutines").Key("numWorkers").Int()

}

func createConfigIfNotExists() {
	if _, err := os.Stat("config.ini"); os.IsNotExist(err) {
		log.Info().Msg("The config.ini file was not found. Creating a new configuration file.")

		cfg := ini.Empty()

		cfg.Section("Server").Key("Host").SetValue("transport.ipay.ua")
		cfg.Section("Server").Key("Port").SetValue("14080")
		cfg.Section("Server").Key("Context").SetValue("load-helper-server-tern/bkl/upload")
		cfg.Section("Server").Key("UseHTTPS").SetValue("true")
		cfg.Section("Server").Key("CertFile").SetValue("server.crt")
		cfg.Section("Server").Key("KeyFile").SetValue("server.key")

		cfg.Section("Auth").Key("Username").SetValue("admin")
		cfg.Section("Auth").Key("Password").SetValue("password")

		cfg.Section("Directories").Key("SendDir").SetValue("./send/")
		cfg.Section("Directories").Key("ArchiveDir").SetValue("./archive/")
		cfg.Section("Directories").Key("LogDir").SetValue("./logs/")

		cfg.Section("File").Key("LogFile").SetValue("app_daily.log")

		cfg.Section("Goroutines").Key("numWorkers").SetValue("2")

		err := cfg.SaveTo("config.ini")
		if err != nil {
			log.Error().Msg(fmt.Sprintf("error creating the config.ini file: %s", err))
		}

		log.Info().Msg("The config.ini file was successfully created with default settings.")
	}
}

func updateConfigIfNeeded() {
	// Загружаем существующий файл конфигурации
	cfg, err := ini.Load("config.ini")
	if err != nil {
		log.Error().Msg(fmt.Sprintf("error loading configuration: %v", err))
	}

	// Проверяем, существует ли секция [Server]
	section, err := cfg.GetSection("Server")
	if err != nil {
		// Если секция не существует, создаем ее
		section, err = cfg.NewSection("Server")
		if err != nil {
			log.Error().Msg(fmt.Sprintf("error creating the [Server] section: %v", err))
		}
		log.Info().Msg("Section [Server] created")
	}

	// Проверяем, существует ли ключ Host
	if section.Key("Host").String() == "" {
		// Если ключ не существует, устанавливаем значение по умолчанию
		section.Key("Host").SetValue("transport.ipay.ua")
		log.Info().Msg("Added value Host = transport.ipay.ua to the [Server] section")
	}

	// Проверяем, существует ли ключ Port
	if section.Key("Port").String() == "" {
		// Если ключ не существует, устанавливаем значение по умолчанию
		section.Key("Port").SetValue("14080")
		log.Info().Msg("Added value Port = 14080 to the [Server] section")
	}

	// Проверяем, существует ли ключ Context
	if section.Key("Context").String() == "" {
		// Если ключ не существует, устанавливаем значение по умолчанию
		section.Key("Context").SetValue("upload")
		log.Info().Msg("Added value Context = upload to the [Server] section")
	}

	// Проверяем, существует ли ключ UseHTTPS
	if section.Key("UseHTTPS").String() == "" {
		// Если ключ не существует, устанавливаем значение по умолчанию
		section.Key("UseHTTPS").SetValue("true")
		log.Info().Msg("Added value UseHTTPS = true to the [Server] section")
	}

	// Проверяем, существует ли ключ CertFile
	if section.Key("CertFile").String() == "" {
		// Если ключ не существует, устанавливаем значение по умолчанию
		section.Key("CertFile").SetValue("server.crt")
		log.Info().Msg("Added value CertFile = server.crt to the [Server] section")
	}

	// Проверяем, существует ли ключ KeyFile
	if section.Key("KeyFile").String() == "" {
		// Если ключ не существует, устанавливаем значение по умолчанию
		section.Key("KeyFile").SetValue("server.key")
		log.Info().Msg("Added value KeyFile = server.key to the [Server] section")
	}

	// Проверяем, существует ли секция [Auth]
	section, err = cfg.GetSection("Auth")
	if err != nil {
		// Если секция не существует, создаем ее
		section, err = cfg.NewSection("Auth")
		if err != nil {
			log.Error().Msg(fmt.Sprintf("error creating the [Auth] section: %v", err))
		}
		log.Info().Msg("Section [Auth] created")
	}

	// Проверяем, существует ли ключ Username
	if section.Key("Username").String() == "" {
		// Если ключ не существует, устанавливаем значение по умолчанию
		section.Key("Username").SetValue("admin")
		log.Info().Msg("Added value Username = admin to the [Auth] section")
	}

	// Проверяем, существует ли ключ Password
	if section.Key("Password").String() == "" {
		// Если ключ не существует, устанавливаем значение по умолчанию
		section.Key("Password").SetValue("password")
		log.Info().Msg("Added value Password = password to the [Auth] section")
	}

	// Проверяем, существует ли секция [Directories]
	section, err = cfg.GetSection("Directories")
	if err != nil {
		// Если секция не существует, создаем ее
		section, err = cfg.NewSection("Directories")
		if err != nil {
			log.Error().Msg(fmt.Sprintf("error creating the [Directories] section: %v", err))
		}
		log.Info().Msg("Section [Directories] created")
	}

	// Проверяем, существует ли ключ SendDir
	if section.Key("SendDir").String() == "" {
		// Если ключ не существует, устанавливаем значение по умолчанию
		section.Key("SendDir").SetValue("./send/")
		log.Info().Msg("Added value SendDir = ./send/ to the [Directories] section")
	}

	// Проверяем, существует ли ключ ArchiveDir
	if section.Key("ArchiveDir").String() == "" {
		// Если ключ не существует, устанавливаем значение по умолчанию
		section.Key("ArchiveDir").SetValue("./archive/")
		log.Info().Msg("Added value ArchiveDir = ./archive/ to the [Directories] section")
	}

	// Проверяем, существует ли ключ LogDir
	if section.Key("LogDir").String() == "" {
		// Если ключ не существует, устанавливаем значение по умолчанию
		section.Key("LogDir").SetValue("./logs/")
		log.Info().Msg("Added value LogDir = ./logs/ to the [Directories] section")
	}

	// Проверяем, существует ли секция [File]
	section, err = cfg.GetSection("File")
	if err != nil {
		// Если секция не существует, создаем ее
		section, err = cfg.NewSection("File")
		if err != nil {
			log.Error().Msg(fmt.Sprintf("error creating the [File] section: %v", err))
		}
		log.Info().Msg("Section [File] created")
	}

	// Проверяем, существует ли ключ LogFile
	if section.Key("LogFile").String() == "" {
		// Если ключ не существует, устанавливаем значение по умолчанию
		section.Key("LogFile").SetValue("app_daily.log")
		log.Info().Msg("Added value LogFile = app_daily.log to the [File] section")
	}

	// Проверяем, существует ли секция [Goroutines]
	section, err = cfg.GetSection("Goroutines")
	if err != nil {
		// Если секция не существует, создаем ее
		section, err = cfg.NewSection("Goroutines")
		if err != nil {
			log.Error().Msg(fmt.Sprintf("error creating the [Goroutines] section: %v", err))
		}
		log.Info().Msg("Section [Goroutines] created")
	}

	// Проверяем, существует ли ключ numWorkers
	if section.Key("numWorkers").String() == "" {
		// Если ключ не существует, устанавливаем значение по умолчанию
		section.Key("numWorkers").SetValue("8")
		log.Info().Msg("Added value numWorkers = 8 to the [Goroutines] section")
	}

	// Сохраняем изменения в конфигурации
	err = cfg.SaveTo("config.ini")
	if err != nil {
		log.Error().Msg(fmt.Sprintf("error saving configuration: %v", err))
	}
}

func createDirectories() {
	dirs := []string{sendDir, archiveDir, logDir}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Error().Msg(fmt.Sprintf("error creating directory %s: %v", dir, err))
		}
	}
}

func main() {
	createDirectories()

	logFilePath := filepath.Join(logDir, logFile)
	logWriter := &lumberjack.Logger{
		Filename:   logFilePath, // Имя файла лога
		MaxSize:    10,          // Максимальный размер файла в МБ
		MaxBackups: 0,           // Максимальное количество резервных файлов
		MaxAge:     0,           // Максимальный возраст резервных файлов в днях
		Compress:   true,        // Сжимать резервные файлы
	}

	// Настройка вывода логов через lumberjack
	log.Logger = log.Output(logWriter)

	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	log.Info().Msg("Starting the file transfer program...")
	log.Info().Msg(fmt.Sprintf("Server address in use: %s", serverAddr))

	// Create the file channel
	fileChan := make(chan string)

	go watchFiles(fileChan)

	// // Start goroutines for sending files
	// startSendGoroutines(5, fileChan) // Adjust the number of goroutines as needed

	// Start goroutines for sending files
	log.Info().Msg(fmt.Sprintf("Starting with %d chanals", numWorkers))
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sendFileWorker(fileChan)
		}()
	}

	// Запись в лог при завершении программы
	exitHandler := func() {
		log.Info().Msg("Terminating the file transfer program...")
		os.Exit(0)
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		exitHandler()
	}()

	wg.Wait()
	//select {} // Бесконечный цикл, чтобы программа не завершалась
}

// Изменяем функцию watchFiles для отправки файлов в канал
func watchFiles(fileChan chan<- string) {
	log.Info().Msg(fmt.Sprintf("Start monitoring folder: %s", sendDir))
	for {
		files, err := os.ReadDir(sendDir)
		if err != nil {
			log.Info().Msg(fmt.Sprintf("error reading the directory: %s", err))
			time.Sleep(1 * time.Second)
			continue
		}

		currentFiles := make(map[string]bool)

		for _, file := range files {
			if !file.IsDir() {
				filePath := filepath.Join(sendDir, file.Name())
				currentFiles[filePath] = true

				fileMutex.Lock()
				if _, exists := fileFirstSeen[filePath]; !exists {
					fileFirstSeen[filePath] = time.Now()
					log.Info().Msg(fmt.Sprintf("New file detected: %s", filePath))
				}
				fileMutex.Unlock()

				if isFileUnchanged(filePath) {
					log.Info().Msg(fmt.Sprintf("The file %s has not been modified for more than 10 seconds. Sending...", filePath))
					fileChan <- filePath // Отправляем файл в канал
				} else {
					log.Info().Msg(fmt.Sprintf("The file %s is not ready for sending yet", filePath))
				}
			}
		}

		// Удаляем из карты файлы, которых больше нет в директории
		fileMutex.Lock()
		for filePath := range fileFirstSeen {
			if !currentFiles[filePath] {
				delete(fileFirstSeen, filePath)
				log.Info().Msg(fmt.Sprintf("The file has been removed from tracking: %s", filePath))
			}
		}
		fileMutex.Unlock()
		time.Sleep(1 * time.Second)
	}
}

func isFileUnchanged(filePath string) bool {
	fileMutex.Lock()
	firstSeen, exists := fileFirstSeen[filePath]
	fileMutex.Unlock()

	if !exists {
		return false
	}

	return time.Since(firstSeen) > 10*time.Second
}

// Функция для обработки отправки файлов
func sendFileWorker(fileChan <-chan string) {
	for filePath := range fileChan {
		err := sendFile(filePath)
		if err != nil {
			log.Error().Msg(fmt.Sprintf("error sending the file: %s", err))
		}
	}
}

func sendFile(filePath string) error {
	log.Info().Msg(fmt.Sprintf("Starting file transfer: %s", filePath))

	// Проверка существования файла перед его открытием
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return fmt.Errorf("file does not exist: %s", filePath)
	}

	file, err := os.Open(filePath)
	if err != nil {
		log.Error().Msg(fmt.Sprintf("error opening the file: %v", err))
		return fmt.Errorf("error opening the file: %v", err)
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)

	// Создаем новый запрос
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	part, err := writer.CreateFormFile("file", filepath.Base(file.Name()))
	if err != nil {
		log.Error().Msg(fmt.Sprintf("error creating the file form: %v", err))
		return fmt.Errorf("error creating the file form: %v", err)
	}

	if _, err = io.Copy(part, file); err != nil {
		log.Error().Msg(fmt.Sprintf("error copying the file to the form: %v", err))
		return fmt.Errorf("error copying the file to the form: %v", err)
	}

	err = writer.Close()
	if err != nil {
		log.Error().Msg(fmt.Sprintf("Error closing the writer: %v", err))
		return fmt.Errorf("error closing the writer: %v", err)
	}

	// Создаем HTTP-клиент с настроенным TLS
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				// InsecureSkipVerify: true,
				// Здесь можно добавить корневые сертификаты, если они нужны
				// RootCAs: rootCAs,
			},
		},
	}

	req, err := http.NewRequest(http.MethodPost, serverAddr, &buf)
	if err != nil {
		log.Error().Msg(fmt.Sprintf("Error creating the request: %v", err))
		return fmt.Errorf("error creating the request: %v", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	// Добавляем заголовок авторизации
	req.SetBasicAuth(username, password)

	resp, err := client.Do(req)
	if err != nil {
		log.Error().Msg(fmt.Sprintf("error sending the request: %v", err))
		return fmt.Errorf("error sending the request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Error().Msg(fmt.Sprintf("error receiving response from server: %s - %s", resp.Status, body))
		return fmt.Errorf("error receiving response from server: %s - %s", resp.Status, body)
	}

	log.Info().Msg(fmt.Sprintf("Successful connection: %s/ -%s- %s", serverAddr, http.MethodPost, resp.Status)) // Логирование успешного соединения

	// Обновление статистики
	fileInfo, err := os.Stat(filePath)
	if err == nil {
		totalFilesSent++
		totalBytesSent += fileInfo.Size()
		lastFileSentName = filepath.Base(filePath)
		lastFileSentTime = time.Now()

		totalBytesSentMB := float64(totalBytesSent) / (1024 * 1024)
		fmt.Printf("File successfully sent: %s | Number of files sent: %d | Total size: %.2f MB | Last file: %s at %s\n",
			lastFileSentName, totalFilesSent, totalBytesSentMB, lastFileSentName, lastFileSentTime.Format(time.RFC3339))
	} else {
		log.Error().Msg(fmt.Sprintf("error getting file info: %v", err))
	}
	// Закрытие файла перед перемещением
	err = file.Close()
	if err != nil {
		log.Error().Msg(fmt.Sprintf("error closing file: %s", err))
		return fmt.Errorf("error closing file")
	}

	// Перемещение файла в архив после успешной отправки
	moveToArchive(filePath) // Убедитесь, что moveToArchive не возвращает ошибку

	return nil
}

func moveToArchive(filePath string) {
	currentDate := time.Now().Format("2006-01-02")
	destDir := filepath.Join(archiveDir, currentDate)

	// Проверка и создание директории
	if _, err := os.Stat(destDir); os.IsNotExist(err) {
		if err := os.MkdirAll(destDir, 0755); err != nil {
			log.Error().Msg(fmt.Sprintf("error creating directory: %s", err))
			return
		}
	}

	destPath := filepath.Join(destDir, filepath.Base(filePath))

	// Обработка конфликтов имен файлов
	counter := 1
	for {
		if _, err := os.Stat(destPath); os.IsNotExist(err) {
			break
		}
		ext := filepath.Ext(destPath)
		baseName := strings.TrimSuffix(filepath.Base(destPath), ext)
		destPath = filepath.Join(destDir, fmt.Sprintf("%s_%d%s", baseName, counter, ext))
		counter++
	}

	err := os.Rename(filePath, destPath)
	if err != nil {
		log.Error().Msg(fmt.Sprintf("error moving file to archive: %s", err))
		return
	}

	log.Info().Msg(fmt.Sprintf("File moved to archive: %s", destPath))

}
