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
	Peers []*Node // соседние узлы для репликации
}

// UpdateData обновляет данные на текущем узле
func (n *Node) UpdateData(value string) {
	n.Mutex.Lock()
	defer n.Mutex.Unlock()
	n.Data.Value = value
	n.Data.Version++
	n.Data.Timestamp = time.Now() // обновляем временную метку
	fmt.Printf("Node %d updated data to %s, version %d, timestamp %v\n", n.ID, n.Data.Value, n.Data.Version, n.Data.Timestamp)
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
			fmt.Printf("Node %d syncs data from Node %d\n", n.ID, peer.ID)
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
		fmt.Printf("Node %d resolved conflict by taking Node %d's data\n", n.ID, peer.ID)
	}
}

func main() {
	// Создаем 5 узлов
	nodes := make([]*Node, 5)
	for i := 0; i < 5; i++ {
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
	go nodes[0].UpdateData("update 1")
	go nodes[1].UpdateData("update 2")
	go nodes[2].UpdateData("update 3")
	go nodes[3].UpdateData("update 4")
	go nodes[4].UpdateData("update 5")

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
		fmt.Printf("Node %d data: %v\n", node.ID, node.ReadData())
	}
}
