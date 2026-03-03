package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Data объект, который будет реплицироваться
type Data struct {
	Value     string
	Version   int
	Timestamp time.Time // добавим временную метку
}

// Node представляет узел в распределенной системе
type Node struct {
	ID    int
	Data  Data
	Mutex sync.Mutex
	Peers []*Node
}

// UpdateData обновляет данные на текущем узле
func (n *Node) UpdateData(value string) {
	n.Mutex.Lock()
	defer n.Mutex.Unlock()
	n.Data.Value = value
	n.Data.Version++
	n.Data.Timestamp = time.Now() // обновляем временную метку
	fmt.Printf("Узел %d обновил данные: %s, версия %d, временная метка %v\n", n.ID, n.Data.Value, n.Data.Version, n.Data.Timestamp)
}

// ReadData читает данные с текущего узла
func (n *Node) ReadData() Data {
	n.Mutex.Lock()
	defer n.Mutex.Unlock()
	return n.Data
}

// SyncData синхронизирует данные с соседними узлами
func (n *Node) SyncData() {
	for _, peer := range n.Peers {
		peer.Mutex.Lock()
		// Проверка версии данных, если версия отличается, передаем данные
		if peer.Data.Version > n.Data.Version {
			n.Data = peer.Data
			fmt.Printf("Узел %d синхронизировал данные с узлом %d\n", n.ID, peer.ID)
		}
		peer.Mutex.Unlock()
	}
}

// SimulateDelay имитирует сетевую задержку
func SimulateDelay() {
	time.Sleep(time.Millisecond * time.Duration(100+rand.Intn(500)))
}

// StartReplication асинхронно синхронизирует данные между узлами
func (n *Node) StartReplication() {
	for _, peer := range n.Peers {
		go func(peer *Node) {
			SimulateDelay()
			n.SyncData()
		}(peer)
	}
}

// ResolveConflict решает конфликт данных, применяя стратегию Last Write Wins
func (n *Node) ResolveConflict(peer *Node) {
	if n.Data.Timestamp.Before(peer.Data.Timestamp) {
		n.Mutex.Lock()
		n.Data = peer.Data
		n.Mutex.Unlock()
		fmt.Printf("Узел %d разрешил конфликт, приняв данные узла %d\n", n.ID, peer.ID)
	}
}

func main() {
	// Создаем узлов
	nodes := make([]*Node, 10)
	for i := 0; i < 10; i++ {
		nodes[i] = &Node{ID: i + 1, Data: Data{Value: "initial", Version: 1, Timestamp: time.Now()}}
	}

	// Устанавливаем связи между узлами для репликации
	for i := 0; i < len(nodes); i++ {
		var peers []*Node
		for j := 0; j < len(nodes); j++ {
			if i != j {
				peers = append(peers, nodes[j])
			}
		}
		nodes[i].Peers = peers
	}

	// Моделируем параллельные записи с конфликтами на каждом узле
	go nodes[0].UpdateData("обновление 1")
	go nodes[1].UpdateData("обновление 2")
	go nodes[2].UpdateData("обновление 3")
	go nodes[3].UpdateData("обновление 4")
	go nodes[4].UpdateData("обновление 5")
	go nodes[4].UpdateData("обновление 6")
	go nodes[4].UpdateData("обновление 7")
	go nodes[4].UpdateData("обновление 8")
	go nodes[4].UpdateData("обновление 9")
	go nodes[4].UpdateData("обновление 10")

	// Ждем завершения операций
	time.Sleep(time.Second * 2)

	// Запускаем синхронизацию данных между узлами
	for _, node := range nodes {
		node.StartReplication()
	}

	// Разрешаем конфликты
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			nodes[i].ResolveConflict(nodes[j])
		}
	}

	// Печатаем данные после синхронизации и разрешения конфликтов
	for _, node := range nodes {
		fmt.Printf("Данные узла %d: %v\n", node.ID, node.ReadData())
	}
}
