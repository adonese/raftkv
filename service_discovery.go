package main

import (
	"encoding/json"
	"net/http"
	"sync"
)

type ServiceDiscovery struct {
	nodes  map[string]string
	leader string
	mu     sync.RWMutex
}

func NewServiceDiscovery() *ServiceDiscovery {
	return &ServiceDiscovery{
		nodes: make(map[string]string),
	}
}

func (sd *ServiceDiscovery) RegisterNode(nodeID, nodeAddr string) {
	sd.mu.Lock()
	defer sd.mu.Unlock()
	sd.nodes[nodeID] = nodeAddr
}

func (sd *ServiceDiscovery) SetLeader(nodeID string) {
	sd.mu.Lock()
	defer sd.mu.Unlock()
	sd.leader = nodeID
}

func (sd *ServiceDiscovery) GetLeader() string {
	sd.mu.RLock()
	defer sd.mu.RUnlock()
	return sd.leader
}

func (sd *ServiceDiscovery) GetNodeAddress(nodeID string) string {
	sd.mu.RLock()
	defer sd.mu.RUnlock()
	return sd.nodes[nodeID]
}

func (sd *ServiceDiscovery) HandleRegister(w http.ResponseWriter, r *http.Request) {
	nodeID := r.URL.Query().Get("nodeId")
	nodeAddr := r.URL.Query().Get("nodeAddr")
	sd.RegisterNode(nodeID, nodeAddr)
	w.WriteHeader(http.StatusOK)
}

func (sd *ServiceDiscovery) HandleSetLeader(w http.ResponseWriter, r *http.Request) {
	nodeID := r.URL.Query().Get("nodeId")
	sd.SetLeader(nodeID)
	w.WriteHeader(http.StatusOK)
}

func (sd *ServiceDiscovery) HandleGetLeader(w http.ResponseWriter, r *http.Request) {
	leader := sd.GetLeader()
	json.NewEncoder(w).Encode(map[string]string{"leader": leader})
}
