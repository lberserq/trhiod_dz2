package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"gitlab.com/n-canter/graph"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"
)

func CheckError(err error) {
	if err != nil {
		log.Println("Error: ", err)
		os.Exit(0)
	}
}

type Msg struct {
	Id      int    `json:"id"`
	MsgType string `json:"type"`
	Sender  int    `json:"sender"`
	Origin  int    `json:"origin"`
	Data    string `json:"data"`
}

type MsgAck struct {
	Id      int    `json:"id"`
	MsgType string `json:"type"`
	Sender  int    `json:"sender"`
	Origin  int    `json:"origin"`
	Data    string `json:"data"`
}

func IsMsgAck(jsonStr []byte) bool {
	ack := MsgAck{}
	err := json.Unmarshal(jsonStr, &ack)
	if err != nil {
		return false
	}
	return ack.MsgType == "notification"
}

func UpdateData(data string) string {

	num, _ := strconv.Atoi(data)
	num = num + 1
	return strconv.Itoa(num)
}

func GetCount(mp map[int]bool) int {
	res := 0
	for _, v := range mp {
		if v {
			res = res + 1
		}
	}
	return res
}

const UDP_PACKET_SIZE = 1024

func CustomDialUDP(LocalAddr, RemoteAddr *net.UDPAddr) *net.UDPConn {
	remConn, err := net.DialUDP("udp", LocalAddr, RemoteAddr)
	nerr, ok := err.(net.Error)
	const maxRetryCount = 50
	for i := 0; i < maxRetryCount && err != nil; i++ {
		remConn, err = net.DialUDP("udp", LocalAddr, RemoteAddr)
		for ok && (nerr.Temporary() || nerr.Timeout()) {
			time.Sleep(1e9)
			remConn, err = net.DialUDP("udp", LocalAddr, RemoteAddr)
			nerr, ok = err.(net.Error)
		}
		time.Sleep(time.Millisecond * 1)
	}
	CheckError(err)
	return remConn
}

func GetId(neighbour graph.Node) int {
	nId, _ := strconv.Atoi(neighbour.String())
	return nId
}

func ReNode(g graph.Graph, id int, exFlag *bool, t uint32, prNo int, startFlag *bool) {
	node, _ := g.GetNode(id)
	neighbors, _ := g.Neighbors(id)

	ServerAddr, err := net.ResolveUDPAddr("udp", ":"+strconv.Itoa(2*node.Port()))
	CheckError(err)
	LocalAddr, err := net.ResolveUDPAddr("udp", ":"+strconv.Itoa(2*node.Port()+1))
	CheckError(err)
	ServerConn, err := net.ListenUDP("udp", ServerAddr)
	CheckError(err)
	log.Println("#", id, "Ports are set up"+strconv.Itoa(node.Port()))
	defer ServerConn.Close()

	buf := make([]byte, UDP_PACKET_SIZE)
	msgMap := make(map[int]bool)
	//notifyMap := make(map[int]map[int]bool)
	msgS := make(map[int]string)
	msgRcvFrom := make(map[int]int)
	msgOrigins := make(map[int]int)

	for !*startFlag {
	}

	for !*exFlag {
		n, _, err := ServerConn.ReadFromUDP(buf)
		CheckError(err)
		log.Println("#", id, " Received ", string(buf[0:n]), " from ", n, " bytes")

		if !IsMsgAck(buf[0:n]) {
			response := Msg{}
			err := json.Unmarshal([]byte(buf[0:n]), &response)
			if err != nil {
				continue
			}
			newData := UpdateData(response.Data)
			if !msgMap[response.Id] {
				msgMap[response.Id] = true
				msgRcvFrom[response.Id] = response.Sender
				msgOrigins[response.Id] = response.Origin
				senderNode, _ := g.GetNode(response.Sender)
				//    notifyMap[response.Id] = make(map[int]bool)
				//  notifyMap[response.Id][response.Sender] = true
				//notifyMap[response.Id][response.Origin] = true
				rresp := response
				rresp.Id = id*prNo + rresp.Id
				rresp.Sender = id
				rresp.Data = newData
				sndBuf, _ := json.Marshal(rresp)
				msgS[response.Id] = string(sndBuf)
				//SendMsgAck to incomer

				ackResponse := MsgAck{
					Id:      response.Id,
					MsgType: "notification",
					Sender:  id,
					Origin:  id,
					Data:    newData}

				RemoteAddr, err := net.ResolveUDPAddr("udp", ":"+strconv.Itoa(2*senderNode.Port()))
				CheckError(err)
				remConn := CustomDialUDP(LocalAddr, RemoteAddr)

				sndBuf, _ = json.Marshal(ackResponse)
				_, err2 := remConn.Write(sndBuf)
				CheckError(err2)
				remConn.Close()
			} else {
				log.Println("#", id, "Dropping message", response)
			}
		} else {
			response := MsgAck{}
			errJ := json.Unmarshal([]byte(buf[0:n]), &response)
			if errJ != nil {
				continue
			}
			/*if (notifyMap[response.Id] == nil) {
			    log.Println("#",id,"Logic error2 occured", response)
			    notifyMap[response.Id] = make(map[int]bool)
			}*/
			//notifyMap[response.Id][response.Sender] = true
			response.Sender = id
			senderNode, _ := g.GetNode(msgRcvFrom[response.Id])
			RemoteAddr, err := net.ResolveUDPAddr("udp", ":"+strconv.Itoa(2*senderNode.Port()))
			CheckError(err)
			remConn := CustomDialUDP(LocalAddr, RemoteAddr)
			sndBuf, _ := json.Marshal(response)
			_, err2 := remConn.Write(sndBuf)
			CheckError(err2)
			remConn.Close()
		}
		for msgId, msgData := range msgS {
			log.Println("#", id, "Begin to send", msgId)
			val := rand.Int() % len(neighbors)
			i := 0
			for GetId(neighbors[val]) == msgRcvFrom[msgId] || val == msgOrigins[msgId] {
				val = rand.Int() % len(neighbors)
			}
			for _, neighbour := range neighbors {

				nId, _ := strconv.Atoi(neighbour.String())
				if i == val && msgRcvFrom[msgId] != nId && msgOrigins[msgId] != nId {
					//if notifyMap[msgId][nId] {
					//    continue
					//}

					RemoteAddr, err := net.ResolveUDPAddr("udp", ":"+strconv.Itoa(2*neighbour.Port()))
					CheckError(err)
					remConn := CustomDialUDP(LocalAddr, RemoteAddr)

					sndBuf := []byte(msgData)

					log.Println("#", id, " Sending to ", nId)

					_, err2 := remConn.Write(sndBuf)
					CheckError(err2)
					remConn.Close()
				}
				i += 1

			}

		}
	}
}

func RootNode(g graph.Graph, id int, exFlag *bool, t uint32, prNo int, BsBig *int, startFlag *bool) {
	node, _ := g.GetNode(id)
	neighbors, _ := g.Neighbors(id)

	ServerAddr, err := net.ResolveUDPAddr("udp", ":"+strconv.Itoa(2*node.Port()))
	CheckError(err)
	LocalAddr, err := net.ResolveUDPAddr("udp", ":"+strconv.Itoa(2*node.Port()+1))
	CheckError(err)
	ServerConn, err := net.ListenUDP("udp", ServerAddr)
	CheckError(err)
	log.Println("#", id, "Port is set up"+strconv.Itoa(node.Port()))
	defer ServerConn.Close()

	buf := make([]byte, UDP_PACKET_SIZE)
	notifyMap := make(map[int]bool)

	rresp := Msg{
		Id:      1,
		MsgType: "multicast",
		Sender:  id,
		Origin:  id,
		Data:    "1"}
	sndBuf, _ := json.Marshal(rresp)

	locFlag := false
	TBig := 0
	for !*startFlag {
	}
	for !locFlag {
		TBig = TBig + 1

		val := rand.Int() % len(neighbors)
		i := 0
		for _, neighbour := range neighbors {
			nId, _ := strconv.Atoi(neighbour.String())
			if i == val {
				log.Println("#", id, " Sending to ", nId)
				RemoteAddr, err := net.ResolveUDPAddr("udp", ":"+strconv.Itoa(2*neighbour.Port()))
				CheckError(err)

				remConn := CustomDialUDP(LocalAddr, RemoteAddr)
				_, err2 := remConn.Write(sndBuf)
				CheckError(err2)
				remConn.Close()
			}
			i += 1
			//time.Sleep(time.Duration(t) * time.Millisecond)
		}

		n, _, err := ServerConn.ReadFromUDP(buf)
		CheckError(err)
		log.Println("#", id, " Received ", string(buf[0:n]), " ",  n, " bytes")
		if !IsMsgAck(buf[0:n]) {
			log.Println("#0 Logic error occured", string(buf[0:n]))
			os.Exit(0)
		}
		response := MsgAck{}
		errJ := json.Unmarshal([]byte(buf[0:n]), &response)
		if errJ != nil {
			continue
		}
		notifyMap[response.Origin] = true
		distNo := GetCount(notifyMap)
		if distNo == prNo-1 {

			locFlag = true
		}
	}
	fmt.Println(TBig)
	*exFlag = true
	*BsBig = TBig
}

func main() {
	rand.Seed(0)
	//rand.Seed(time.Now().UTC().UnixNano())
	nPtr := flag.Int("n", 10, "number of nodes")
	flag.Parse()
	n := *nPtr
	log.Println("N==", n)
	g := graph.Generate(n, 4, 20, 25000)
	flag := false
	x := 0
	startFlag := false
	for i := 1; i < n; i++ {
		go ReNode(g, i, &flag, 1, n, &startFlag)
	}

	go RootNode(g, 0, &flag, 1, n, &x, &startFlag)

	startFlag = true
	for {
		if flag {
			break
		}
		time.Sleep(time.Millisecond * 50)
	}
	fmt.Println(x)
	log.Println(x)

}
