package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"fmt"
	"gopkg.in/ini.v1"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	lastLogDate      time.Time
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

const (
	maxLogSize = 2 * 1024 * 1024 // 2 MB
)

func init() {
	createConfigIfNotExists()
	updateConfigIfNeeded()

	var err error
	cfg, err := ini.Load("config.ini")
	if err != nil {
		log.Fatal("Error loading the config.ini file: ", err)
	}

	useHTTPS, _ = cfg.Section("Server").Key("UseHTTPS").Bool()
	protocol := "http"
	if useHTTPS {
		protocol = "https"
		certFile = cfg.Section("Server").Key("CertFile").String()
		keyFile = cfg.Section("Server").Key("KeyFile").String()
	}

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
		log.Println("The config.ini file was not found. Creating a new configuration file.")

		cfg := ini.Empty()

		cfg.Section("Server").Key("Host").SetValue("transport.ipay.ua")
		cfg.Section("Server").Key("Port").SetValue("38080")
		cfg.Section("Server").Key("Context").SetValue("load-helper-server-zhit/bkl/upload")
		cfg.Section("Server").Key("UseHTTPS").SetValue("true")
		cfg.Section("Server").Key("CertFile").SetValue("server.crt")
		cfg.Section("Server").Key("KeyFile").SetValue("server.key")

		cfg.Section("Auth").Key("Username").SetValue("user")
		cfg.Section("Auth").Key("Password").SetValue("password")

		cfg.Section("Directories").Key("SendDir").SetValue("./send/")
		cfg.Section("Directories").Key("ArchiveDir").SetValue("./archive/")
		cfg.Section("Directories").Key("LogDir").SetValue("./logs/")

		cfg.Section("File").Key("LogFile").SetValue("app_daily.log")

		cfg.Section("Goroutines").Key("numWorkers").SetValue("8")

		err := cfg.SaveTo("config.ini")
		if err != nil {
			log.Fatal("Error creating the config.ini file: ", err)
		}

		log.Println("The config.ini file was successfully created with default settings.")
	}
}

func updateConfigIfNeeded() {
	// Загружаем существующий файл конфигурации
	cfg, err := ini.Load("config.ini")
	if err != nil {
		log.Fatalf("Error loading configuration: %v", err)
	}

	// Проверяем, существует ли секция [Server]
	section, err := cfg.GetSection("Server")
	if err != nil {
		// Если секция не существует, создаем ее
		section, err = cfg.NewSection("Server")
		if err != nil {
			log.Fatalf("Error creating the [Server] section: %v", err)
		}
		log.Println("Section [Server] created")
	}

	// Проверяем, существует ли ключ Host
	if section.Key("Host").String() == "" {
		// Если ключ не существует, устанавливаем значение по умолчанию
		section.Key("Host").SetValue("transport.ipay.ua")
		log.Println("Added value Host = transport.ipay.ua to the [Server] section")
	}

	// Проверяем, существует ли ключ Port
	if section.Key("Port").String() == "" {
		// Если ключ не существует, устанавливаем значение по умолчанию
		section.Key("Port").SetValue("14080")
		log.Println("Added value Port = 14080 to the [Server] section")
	}

	// Проверяем, существует ли ключ Context
	if section.Key("Context").String() == "" {
		// Если ключ не существует, устанавливаем значение по умолчанию
		section.Key("Context").SetValue("upload")
		log.Println("Added value Context = upload to the [Server] section")
	}

	// Проверяем, существует ли ключ UseHTTPS
	if section.Key("UseHTTPS").String() == "" {
		// Если ключ не существует, устанавливаем значение по умолчанию
		section.Key("UseHTTPS").SetValue("true")
		log.Println("Added value UseHTTPS = true to the [Server] section")
	}

	// Проверяем, существует ли ключ CertFile
	if section.Key("CertFile").String() == "" {
		// Если ключ не существует, устанавливаем значение по умолчанию
		section.Key("CertFile").SetValue("server.crt")
		log.Println("Added value CertFile = server.crt to the [Server] section")
	}

	// Проверяем, существует ли ключ KeyFile
	if section.Key("KeyFile").String() == "" {
		// Если ключ не существует, устанавливаем значение по умолчанию
		section.Key("KeyFile").SetValue("server.key")
		log.Println("Added value KeyFile = server.key to the [Server] section")
	}

	// Проверяем, существует ли секция [Goroutines]
	section, err = cfg.GetSection("Goroutines")
	if err != nil {
		// Если секция не существует, создаем ее
		section, err = cfg.NewSection("Goroutines")
		if err != nil {
			log.Fatalf("Error creating the [Goroutines] section: %v", err)
		}
		log.Println("Section [Goroutines] created")
	}

	// Проверяем, существует ли ключ numWorkers
	if section.Key("numWorkers").String() == "" {
		// Если ключ не существует, устанавливаем значение по умолчанию
		section.Key("numWorkers").SetValue("8")
		log.Println("Added value numWorkers = 8 to the [Goroutines] section")
	}

	// Сохраняем изменения в конфигурации
	err = cfg.SaveTo("config.ini")
	if err != nil {
		log.Fatalf("Error saving configuration: %v", err)
	}
}

func main() {
	createDirectories()
	setupLogging()
	logWithCheck(fmt.Sprint("Starting the file transfer program..."))
	log.Printf("Server address in use: %s\n", serverAddr)

	// Create the file channel
	fileChan := make(chan string)

	// // Start goroutines for sending files
	// startSendGoroutines(5, fileChan) // Adjust the number of goroutines as needed

	// Start goroutines for sending files
	log.Printf("Запуск %d потоков\n", numWorkers)
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
		log.Println("Terminating the file transfer program...")
		os.Exit(0)
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		exitHandler()
	}()

	go watchFiles(fileChan)
	select {} // Бесконечный цикл, чтобы программа не завершалась
}

func createDirectories() {
	dirs := []string{sendDir, archiveDir, logDir}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatalf("Error creating directory %s: %v", dir, err)
		}
	}
}

func setupLogging() {
	// Создаем директорию для логов, если она не существует
	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		os.Mkdir(logDir, 0755)
	}

	// Проверяем, существует ли лог-файл
	logFilePath := filepath.Join(logDir, logFile)
	if _, err := os.Stat(logFilePath); err == nil {
		// Лог-файл существует, проверяем его размер
		fileInfo, err := os.Stat(logFilePath)
		if err == nil && fileInfo.Size() > int64(maxLogSize) {
			// Если размер больше 2 МБ, выполняем ротацию
			rotateLogs(logFilePath, time.Now())
		}
	}

	// Открываем новый лог-файл для записи
	file, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("Error opening the log file:", err)
	}
	log.SetOutput(file)
}

// Функция для проверки размера лог-файла и архивации
func checkLogSizeAndRotate() {
	logFilePath := filepath.Join(logDir, logFile)
	fileInfo, err := os.Stat(logFilePath)
	if err == nil && fileInfo.Size() > int64(maxLogSize) {
		rotateLogs(logFilePath, time.Now())
	}
}

// Функция для записи в лог с проверкой размера
func logWithCheck(message string) {
	checkLastRowAndRotateBeforeWrite() // Проверяем последнюю строку перед записью
	log.Println(message)
	checkLogSizeAndRotate() // Проверяем размер после записи
}

// Функция для проверки даты последнего сообщения в лог-файле перед записью
func checkLastRowAndRotateBeforeWrite() {
	logFilePath := filepath.Join(logDir, logFile)
	lines, err := readLastLines(logFilePath, 2)
	if err == nil && len(lines) > 0 {
		lastLine := lines[len(lines)-1]
		logDate, err := time.Parse("2006/01/02 15:04:05", lastLine[:19])
		if err == nil && logDate.Before(time.Now().Truncate(24*time.Hour)) {
			// Дата последней записи меньше текущей, выполняем ротацию
			rotateLogs(logFilePath, logDate)
		}
	}
}

// Функция для ротации логов
func rotateLogs(logFilePath string, logDate time.Time) {
	// Создаем директорию для старых логов
	oldLogDir := filepath.Join(logDir, logDate.Format("2006-01-02"))
	os.MkdirAll(oldLogDir, 0755)

	// Перемещаем старый лог-файл
	archivedLogPath := filepath.Join(oldLogDir, logFile)
	err := os.Rename(logFilePath, archivedLogPath)
	if err != nil {
		log.Println("Error moving the log file:", err)
		return
	}

	// Определяем имя архива
	baseTarFileName := logDate.Format("2006-01-02")
	tarFileName := filepath.Join(oldLogDir, fmt.Sprintf("%s-1.tar.gz", baseTarFileName))

	// Проверяем существование архива и увеличиваем номер, если необходимо
	n := 1
	for {
		if _, err := os.Stat(tarFileName); os.IsNotExist(err) {
			break // Файл не существует, можно использовать это имя
		}
		n++
		tarFileName = filepath.Join(oldLogDir, fmt.Sprintf("%s-%d.tar.gz", baseTarFileName, n))
	}

	// Архивируем лог-файл
	cmd := exec.Command("tar", "-czf", tarFileName, "-C", oldLogDir, logFile)
	if err := cmd.Run(); err != nil {
		log.Println("Error archiving logs:", err)
		return
	}

	// Удаляем старый лог-файл после успешного архивирования
	os.Remove(archivedLogPath)
	log.Println("Logs successfully archived to:", tarFileName)

	// Инициализация нового лога
	setupLogging()
}

func readLastLines(filePath string, n int) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) > n {
			lines = lines[1:] // Удаляем старые строки, если их больше n
		}
	}
	return lines, scanner.Err()
}

// Функция для обработки отправки файлов
func sendFileWorker(fileChan <-chan string) {
	for filePath := range fileChan {
		err := sendFile(filePath)
		if err != nil {
			log.Println("Error sending the file:", err)
		}
	}
}

// Изменяем функцию watchFiles для отправки файлов в канал
func watchFiles(fileChan chan<- string) {
	log.Println("Starting to monitor the folder:", sendDir)
	for {
		files, err := os.ReadDir(sendDir)
		if err != nil {
			log.Println("Error reading the directory:", err)
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
					logWithCheck(fmt.Sprintf("New file detected: %s", filePath))
				}
				fileMutex.Unlock()

				if isFileUnchanged(filePath) {
					log.Printf("The file %s has not been modified for more than 10 seconds. Sending...\n", filePath)
					fileChan <- filePath // Отправляем файл в канал
				} else {
					log.Printf("The file %s is not yet ready for sending\n", filePath)
				}
			}
		}

		// Удаляем из карты файлы, которых больше нет в директории
		fileMutex.Lock()
		for filePath := range fileFirstSeen {
			if !currentFiles[filePath] {
				delete(fileFirstSeen, filePath)
				log.Printf("The file has been removed from tracking: %s\n", filePath)
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

	return time.Since(firstSeen) > 2*time.Second
}

func sendFile(filePath string) error {
	log.Printf("Starting file transfer: %s", filePath)

	// Проверка существования файла перед его открытием
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return fmt.Errorf("File does not exist: %s", filePath)
	}

	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("Error opening the file: %v", err)
		return fmt.Errorf("Error opening the file: %v", err)
	}
	defer file.Close()

	// Создаем новый запрос
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	part, err := writer.CreateFormFile("file", filepath.Base(file.Name()))
	if err != nil {
		log.Printf("Error creating the file form: %v", err)
		return fmt.Errorf("Error creating the file form: %v", err)
	}

	if _, err = io.Copy(part, file); err != nil {
		log.Printf("Error copying the file to the form: %v", err)
		return fmt.Errorf("Error copying the file to the form: %v", err)
	}

	err = writer.Close()
	if err != nil {
		log.Printf("Error closing the writer: %v", err)
		return fmt.Errorf("Error closing the writer: %v", err)
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
		log.Printf("Error creating the request: %v", err)
		return fmt.Errorf("Error creating the request: %v", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	// Добавляем заголовок авторизации
	req.SetBasicAuth(username, password)

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error sending the request: %v", err)
		return fmt.Errorf("Error sending the request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		log.Printf("Error receiving response from the server: %s - %s", resp.Status, body)
		return fmt.Errorf("Error receiving response from the server: %s - %s", resp.Status, body)
	}

	log.Printf("Successful connection: %s/ -%s- %s", serverAddr, http.MethodPost, resp.Status) // Логирование успешного соединения

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
		log.Printf("Error getting file info: %v", err)
	}
	// Закрытие файла перед перемещением
	err = file.Close()
	if err != nil {
		log.Println("Error closing file:", err)
		return fmt.Errorf("Error closing file")
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
			log.Println("Error creating directory:", err)
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
		log.Println("Error moving file to archive:", err)
		return
	}

	logWithCheck(fmt.Sprintf("File moved to archive: %s", destPath))

}
