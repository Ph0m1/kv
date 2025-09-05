package common

import (
	"fmt"
	"log"
	"os"
	"time"
)

// LogLevel 日志级别
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
)

var levelNames = map[LogLevel]string{
	DEBUG: "DEBUG",
	INFO:  "INFO",
	WARN:  "WARN",
	ERROR: "ERROR",
}

// Logger 简单的日志器
type Logger struct {
	level  LogLevel
	logger *log.Logger
	file   *os.File
}

var defaultLogger *Logger

func init() {
	defaultLogger = NewLogger(INFO, os.Stdout)
}

// NewLogger 创建新的日志器
func NewLogger(level LogLevel, output *os.File) *Logger {
	return &Logger{
		level:  level,
		logger: log.New(output, "", 0),
		file:   output,
	}
}

// NewFileLogger 创建文件日志器
func NewFileLogger(level LogLevel, filePath string) (*Logger, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}
	return NewLogger(level, file), nil
}

// SetLevel 设置日志级别
func (l *Logger) SetLevel(level LogLevel) {
	l.level = level
}

// log 内部日志方法
func (l *Logger) log(level LogLevel, format string, args ...interface{}) {
	if level < l.level {
		return
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	levelName := levelNames[level]
	message := fmt.Sprintf(format, args...)

	l.logger.Printf("[%s] [%s] %s", timestamp, levelName, message)
}

// Debug 调试日志
func (l *Logger) Debug(format string, args ...interface{}) {
	l.log(DEBUG, format, args...)
}

// Info 信息日志
func (l *Logger) Info(format string, args ...interface{}) {
	l.log(INFO, format, args...)
}

// Warn 警告日志
func (l *Logger) Warn(format string, args ...interface{}) {
	l.log(WARN, format, args...)
}

// Error 错误日志
func (l *Logger) Error(format string, args ...interface{}) {
	l.log(ERROR, format, args...)
}

// Close 关闭日志器
func (l *Logger) Close() error {
	if l.file != os.Stdout && l.file != os.Stderr {
		return l.file.Close()
	}
	return nil
}

// 全局日志函数
func Debug(format string, args ...interface{}) {
	defaultLogger.Debug(format, args...)
}

func Info(format string, args ...interface{}) {
	defaultLogger.Info(format, args...)
}

func Warn(format string, args ...interface{}) {
	defaultLogger.Warn(format, args...)
}

func Error(format string, args ...interface{}) {
	defaultLogger.Error(format, args...)
}

// SetDefaultLogger 设置默认日志器
func SetDefaultLogger(logger *Logger) {
	defaultLogger = logger
}

// GetDefaultLogger 获取默认日志器
func GetDefaultLogger() *Logger {
	return defaultLogger
}
