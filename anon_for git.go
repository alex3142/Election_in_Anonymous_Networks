// ADVANCED DISTRIBUTED SYSTEMS

package main

// We import some useful modules

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// We create the structure "Node". A "Node" will consist of an IP Address, Port, ID, what it receives and an id

type Node struct {
	ip              string
	port            string
	nodeId          string
	messageReceived bool
}

/*
type IncomingMessage struct {
	idOfSender string
	message    string
}
*/

//this struct stores the algorithm data (n,idp,0)
type AlgorithmDataObj struct {
	n              int
	id             int
	thirdParameter int
	//not explicitly part of the algoritm
	// but would be known if using
	//tcp/ip and will be useful later
	idOfSender string
}

type SpeakingChannels struct {
	id        string
	myChannel chan string
}

//utility to ensure we are connected to all our neighbours
// not part of the algorithm
var nConnections int

var restartAlgorithm bool

// We create the main function

func main() {

	// the random numbers are not random
	rand.Seed(time.Now().UTC().UnixNano())

	restartAlgorithm = false

	// Print Starting Process in the shell

	fmt.Println("Starting Process...")

	//flag for if algoithm has completed
	//var algorithmComplete bool
	//algorithmComplete = false

	//for testing to stop infinite loops of messages
	var nMessages int
	nMessages = 0

	//we will use this if we are an initiator to
	//otherwise we will ignore it
	var id_p int

	//declare round number
	var n int
	n = -1

	//declare number of nodes
	var N int

	// these channels will be used to send data
	// into the clients to be sent to the servers
	var channelsToSend []SpeakingChannels

	// also we will have one channel we are listneing on
	// which will receive the data from other clients
	// and there is  abuffer of 5, seemed like a good idea
	// to have a buffer, 5 is a pretty number
	channelToReceive := make(chan string)

	// make sure we have two of them
	// get arguments from command prompt

	arguments := os.Args

	if len(arguments) != 3 {

		fmt.Println("Please provide a configuration file and number of nodes!")

		return

	}

	// assign imputs to the correct type expected by
	// the client and server functions

	filename := arguments[1]

	//randomly select the id of the node given the
	// input argument

	//N = strconv.ParseInt(arguments[2], 0, 64)

	i, _ := strconv.ParseInt(arguments[2], 0, 64)
	// Check err
	N = int(i)

	// We read the configuration file
	iamNode, neighbours, initiatorBool := readFile(filename)

	// instantiate our local memory of the node
	// which it will use to compare the new
	// incoming informatoin to
	// the algorithm doesn't ppear to suggest initial
	// values for non initiators
	// so i've set them all to -1
	// seemed as good as idea as any...
	var currentNodeMemory AlgorithmDataObj

	// provide some useful information to the user
	fmt.Printf("I am %s:%s \nisInitiator is: %t \nAll my neighbours are: %v \n", iamNode.ip, iamNode.port, initiatorBool, neighbours)

	// We execute the server function for the node
	// with the channel it will put the received data into
	go server(iamNode, channelToReceive)

	//create clients and channels to communicate with them
	for i := 0; i < len(neighbours); i++ {

		// this variable will keep track of which channel sends data to which neighbour
		channelsToSend = append(channelsToSend, SpeakingChannels{neighbours[i].ip + ":" + neighbours[i].port, make(chan string)})

		// create servers with all the information needed to send data
		// this channel will be used to provide it data to send
		go client(iamNode, neighbours[i], channelsToSend[i].myChannel)

	}

	// utility to check all connections have been successful
	// not part of the algorithm but allows it to being
	// when the network is ready
	for {
		if nConnections == len(neighbours) {

			//note due to the asyncronous nature of
			// go this may get printed after
			// messages which may have happened
			// previous to this, still i found it useful
			// to know what's going on in the program
			fmt.Println("##### ready ####")
			goto StartAlgorithm
		}
	}
StartAlgorithm:

	fmt.Println("##### starting algorithm ####")
	//increment round (initialised to -1)
	n = n + 1

	fmt.Println("round number " + strconv.Itoa(n))

	//set id_p (need to add one as initially in the range 0,n)
	id_p = rand.Intn(N-1) + 1

	//set the current memory for this node
	if initiatorBool {
		currentNodeMemory = AlgorithmDataObj{n, id_p, 0, ""}
	} else {
		currentNodeMemory = AlgorithmDataObj{-1, -1, 0, ""}
	}

	for i := 0; i < len(neighbours); i++ {

		// this initil messge for initiators is
		// provided in the assignment
		neighbours[i].messageReceived = false

	}

	//now the clients and servers have been created
	//we can loop andprocess messages

	if initiatorBool {
		//send initial message

		fmt.Println("my id " + strconv.Itoa(id_p))

		//just give a liitle time for the
		//network to initiate
		time.Sleep(4000 * time.Millisecond)

		//send messages to neighbours
		for i := 0; i < len(channelsToSend); i++ {

			//fmt.Println("MAIN initiator sending message to " + channelsToSend[i].id)

			// this initil messge for initiators is
			// provided in the assignment
			channelsToSend[i].myChannel <- AlgObjToString(AlgorithmDataObj{n, id_p, 0, iamNode.nodeId})

		}
	}

	// useful information for the user
	//fmt.Println("handling messages")

	// essentially this function runs the algorithm
	// it is in charge of taking the output of the received information
	// from the server and supplying clients with messages to send
	go HandleMessages(channelsToSend, channelToReceive, initiatorBool, neighbours, currentNodeMemory, iamNode, N)

	// if you don't have something being calculated in the
	// main function then go ends the program
	// so this is that thing
	// i might code this to provide an upper limit to the number of
	// messages at some point, maybe not
	for {
		if restartAlgorithm {
			restartAlgorithm = false
			goto StartAlgorithm

		}
		if nMessages > 1000 {
			fmt.Println("ending")
			return
		}
	}

}

func client(myNode Node, nbour Node, myChannel chan string) {
	//this function is in charge of sending messages provided by
	// the 'HandleMessages function in the 'myChannel' channel,
	// to the 'nbour' node provided in the input

	// declare required vars
	// go REALLY does not like
	// you if yo don't declare vars
	var conn net.Conn
	var err error
	var messageToSend string

	for {
		conn, err = net.Dial("tcp", nbour.ip+":"+nbour.port)

		// informs the user for which node we are looking
		fmt.Println("CHECKNBOURS Looking for " + nbour.ip + ":" + nbour.port)

		// sleeps for a bit so as not to
		// spam the screen with messages
		time.Sleep(100 * time.Millisecond)

		if err == nil {
			// if there is no error then we are connected

			// send an arbitrary starting message
			// not sure if i need this so may remove later

			messageToSend = AlgObjToString(AlgorithmDataObj{-10, -10, -10, myNode.nodeId})

			conn.Write([]byte(messageToSend))

			// inform the user
			fmt.Println("CHECKNBOURS found " + nbour.ip + ":" + nbour.port)

			//increment connections so we know when we have connected to all
			nConnections = nConnections + 1

			break

		}
	}

	//conn, err = net.Dial("tcp", nbour.ip+":"+nbour.port)

	for {

		// the the message to send out of the channel
		messageToSend := <-myChannel

		conn, err = net.Dial("tcp", nbour.ip+":"+nbour.port)
		// this loop controls the sending of algorithm messages

		// useful info for the user
		//fmt.Println("CLIENT awaiting message to send")

		// send the message
		conn.Write([]byte(messageToSend))
		conn.Close()

		fmt.Println("CLIENT SENT ALG MESSAGE = " + messageToSend + "to " + nbour.ip + ":" + nbour.port)

		// useful information for the user
		//fmt.Printf("CLIENT Message text: " + messageToSend. + "\n")
		//fmt.Println("CLIENT Message sent to... %s:%s \n", nbour.ip, nbour.port)

	}

}

// We create the function server. This function is in charge of enabling the nodes to listen

func server(s Node, myChannel chan string) {

	// Print "Launshing server..." in the shell

	fmt.Printf("SERVER Launching server... %s:%s \n", s.ip, s.port)

	// Start listening

	// We do not stop listening until the surrounding function returns (finishes)
	ln, _ := net.Listen("tcp", s.ip+":"+s.port)

	for {

		// Accept connections
		//fmt.Println("SERVER waiting at ln.Accept()")
		conn, err := ln.Accept()

		if err != nil {
			panic(err)
		}

		message, err := bufio.NewReader(conn).ReadString('\n')
		conn.Close()

		fmt.Println("SERVER message received = " + message)
		if err != nil {
			fmt.Println("the server is broken")
			panic(err)
		}
		//message = strings.TrimSuffix(message, "\n")
		//fmt.Println(utf8.RuneCountInString(message))

		//fmt.Println("SERVER message = " + message)
		//fmt.Println("SERVER (raw) message received (in server) = " + message)

		// If the mmessage is not empty, then print it in the shell

		if string(message) != "" {
			//fmt.Println("SERVER message put in channel")
			myChannel <- message

			//fmt.Println("SERVER (processed) message received (in server) ")
		}

	}

}

func HandleMessages(mySpeakingChannels []SpeakingChannels, myListenChannel chan string, initiatorBool bool, nbours []Node, myCurrentNodeMemory AlgorithmDataObj, myNode Node, N int) {

	//fmt.Println("HANDLE MESSAGE BEGINNING")
	// this function handles the running of the algorithm
	// it takes in all the channels used to send data,
	// the channel used to receive data, a bool flag
	// showing whether this node is an initiator,
	//  a struct containing all the nodes , and the
	// 'memory' for this current node

	// id of the parent
	var parentId string

	// stores the message to send to neighbours
	var messageToSendAlgObj AlgorithmDataObj

	var messageToSendString string

	//message count, might be useful later, maybe i will remove
	var messageCount int

	var currentIncomingAlgObj AlgorithmDataObj

	var continueAlg bool

	var tempAlgReadyDecisionBool bool

	messageCount = 0

	// loop to run the algorithm
	for {

		//wait for an incomming message
		// note, since the initiator will have sent their
		// messages before reaching this point
		// there will always be a message in the system
		// (assuming there is an initiator)
		//fmt.Println("HANDLING awaiting message")
		currentIncoming := <-myListenChannel

		messageCount = messageCount + 1
		//fmt.Println("HANDLING message received = " + currentIncoming)

		currentIncomingAlgObj = StringToAlgObj(currentIncoming)

		if currentIncomingAlgObj.n == -10 {
			//fmt.Println("initial message ignoring")

			//then we know this was an initial connection message
			// as we arbitrarily chose -10 for that message
			// so reduce the count and skip everything and
			// wait for the next message from the algorithm

			messageCount = messageCount - 1
		} else {
			//fmt.Println("algorithm message processing")

			//note that a message was received
			//used to keep track of echo
			nbours = RecordMessageReceived(nbours, currentIncomingAlgObj.idOfSender)

			//////////////////////////////////////////////////////////////////
			//////////////////////////////////////////////////////////////////
			//////////////         run the algorithm     /////////////////////
			//////////////////////////////////////////////////////////////////
			//////////////////////////////////////////////////////////////////

			if currentIncomingAlgObj.n > myCurrentNodeMemory.n || (currentIncomingAlgObj.n == myCurrentNodeMemory.n && currentIncomingAlgObj.id > myCurrentNodeMemory.id) {
				parentId = currentIncomingAlgObj.idOfSender

				// now stop being an initiator and join in as a normal node
				initiatorBool = false

				//reset the current memory
				myCurrentNodeMemory.n = currentIncomingAlgObj.n
				myCurrentNodeMemory.id = currentIncomingAlgObj.id
				myCurrentNodeMemory.thirdParameter = 0
				//reset knowledge from previous wave
				nbours = RecordMessagePurgeAll(nbours)
				//but ensure this current message is still recorded
				nbours = RecordMessageReceived(nbours, currentIncomingAlgObj.idOfSender)

				// print useful infnbours
				//fmt.Println("MAIN parent = " + currentIncomingAlgObj.idOfSender)
				continueAlg = true

			}

			if currentIncomingAlgObj.n < myCurrentNodeMemory.n || (currentIncomingAlgObj.n == myCurrentNodeMemory.n && currentIncomingAlgObj.id < myCurrentNodeMemory.id) {

				// print useful info
				//fmt.Println("message purged = " + currentIncoming)
				nbours = RecordMessagePurge(nbours, currentIncomingAlgObj.idOfSender)
				continueAlg = false

			}

			if currentIncomingAlgObj.n == myCurrentNodeMemory.n && currentIncomingAlgObj.id == myCurrentNodeMemory.id {

				// run normal echo
				continueAlg = true

			}
			if continueAlg {

				tempAlgReadyDecisionBool = ReadyToMessageParent(nbours, parentId)

				fmt.Println("parentId = " + parentId)
				//fmt.Println("currentIncomingAlgObj.idOfSender = " + currentIncomingAlgObj.idOfSender)
				/*
					if currentIncomingAlgObj.idOfSender == parentId {
						fmt.Println("fuck yeah")
					}
				*/

				if parentId == currentIncomingAlgObj.idOfSender {
					//send messages to all children
					//fmt.Println("sending to everyone")

					//fmt.Println("len(mySpeakingChannels) = " + strconv.Itoa(len(mySpeakingChannels)))

					for i := 0; i < len(mySpeakingChannels); i++ {
						//fmt.Println("MAIN tring to send message")
						if mySpeakingChannels[i].id != parentId {
							//maybe we want to do something different for
							// the parent...

							messageToSendAlgObj = AlgorithmDataObj{currentIncomingAlgObj.n, currentIncomingAlgObj.id, currentIncomingAlgObj.thirdParameter, myNode.nodeId}

							messageToSendString = AlgObjToString(messageToSendAlgObj)

							mySpeakingChannels[i].myChannel <- messageToSendString

						}
					}

					// need to split these up incase the parent
					// arbirarily is the first in the list
					// if that's the case the children nodes will not have time to
					// update, this allows for that.
					for i := 0; i < len(mySpeakingChannels); i++ {
						if mySpeakingChannels[i].id == parentId {

							if tempAlgReadyDecisionBool {
								// ReadyToMessageParent checks if
								// (following the echo algorithm)
								// we should message the parent
								// this is needed here incase the parent is the only
								// neighbour of the node

								//fmt.Println("MESSAGING PARENT")

								// increment the thridParameter by 1 to include the current node in the sub tree
								messageToSendAlgObj = AlgorithmDataObj{currentIncomingAlgObj.n, currentIncomingAlgObj.id, currentIncomingAlgObj.thirdParameter + 1, myNode.nodeId}

								messageToSendString = AlgObjToString(messageToSendAlgObj)

								mySpeakingChannels[i].myChannel <- messageToSendString

							}

						}

					}
				} else {
					fmt.Println("decision to be made here")
					//then it must be a message from a child so update te current node memory
					myCurrentNodeMemory.thirdParameter = myCurrentNodeMemory.thirdParameter + currentIncomingAlgObj.thirdParameter
					fmt.Println("myCurrentNodeMemory.thirdParameter = " + strconv.Itoa(myCurrentNodeMemory.thirdParameter))
					//fmt.Println("restarting")
					// then need to check if the node is an initiator
					if tempAlgReadyDecisionBool && initiatorBool {

						if N == myCurrentNodeMemory.thirdParameter+1 {
							// plus one here to include current node
							fmt.Println("!!!!!!!!DECIDE!!!!!!!!")
						} else {
							fmt.Println("restarting")
							fmt.Println("currentIncomingAlgObj.thirdParameter = " + strconv.Itoa(currentIncomingAlgObj.thirdParameter))
							restartAlgorithm = true
							return
						}

						/*
							//message everyone to stop
							for i := 0; i < len(mySpeakingChannels); i++ {
								fmt.Println("MAIN tring to send message")
								messageToSend = "STOP"
								fmt.Println("MAIN tring to send STOP to children: " + messageToSend)
								mySpeakingChannels[i].myChannel <- messageToSend

							}
						*/

					} else if tempAlgReadyDecisionBool {
						// then finally maybe we are not an initiator,
						// and the message has not come from our parent,
						// but we have all messages from our children
						// so as per the echo algorithm we are ready
						// to reply to our parent

						// then mesage parent
						for i := 0; i < len(mySpeakingChannels); i++ {
							fmt.Println("MAIN trying to send message to parent")
							if mySpeakingChannels[i].id == parentId {

								// increment the thridParameter by 1 to include the current node in the sub tree
								messageToSendAlgObj = AlgorithmDataObj{currentIncomingAlgObj.n, currentIncomingAlgObj.id, myCurrentNodeMemory.thirdParameter + 1, myNode.nodeId}

								messageToSendString = AlgObjToString(messageToSendAlgObj)

								mySpeakingChannels[i].myChannel <- messageToSendString

							}
						}

					}
				}
				//else do nothing and wait for more messages
			}
		}
		/////////////////////////////////////
		/////////////////////////////////////
		/////////////////////////////////////
		/////////////////////////////////////

		//legacy will remove
		if messageToSendString == "STOP" {
			fmt.Println("MAIN Successful end")
			os.Exit(0)
			return
		}
	}

}

// We create the function readFile. This function is in charge of reading the configuration file

func readFile(fileName string) (Node, []Node, bool) {

	var isInitiator bool
	var iamNode Node
	var neighbours []Node

	//assume we are not an initiator
	//unless we see evidence to the contrary
	isInitiator = false

	// We open the configuration file

	f, _ := os.Open(fileName)

	// We do not close the configuration file until the surrounding function returns (finishes)

	defer f.Close()

	// We return a new Reader whose buffer has at least the specified size (2 * 1024)

	r := bufio.NewReaderSize(f, 2*1024)

	// ReadLine tries to return a single line, not including the end-of-line bytes. If the line was too long for the buffer then
	// isPrefix is set and the beginning of the line is returned. The rest of the line will be returned from future calls. isPrefix
	// will be false when returning the last fragment of the line. The returned buffer is only valid until the next call to ReadLine.
	// ReadLine either returns a non-nil line or it returns an error, never both.

	line, isPrefix, err := r.ReadLine()

	i := 1

	// When there is no error and isPrefix is false then do

	for err == nil && !isPrefix {

		// We convert the line to string

		s := string(line)

		// The first line corresponds to the default local IP, Port, ID

		if i == 1 {

			// Get information for local node

			// We split the string by ":"

			t := strings.Split(s, ":")

			if len(t) > 2 && t[2] == "*" {

				// Check if the node is an initiator

				isInitiator = true
				iamNode = Node{t[0], t[1], t[0] + ":" + t[1], false}

			} else {
				//the node is not an initiator, there
				iamNode = Node{t[0], t[1], t[0] + ":" + t[1], false}

			}

		} else {

			// Get information for rest of nodes

			k := strings.Split(s, ":")

			// Get neighbours

			neighbours = append(neighbours, Node{k[0], k[1], k[0] + ":" + k[1], false})

		}

		i++

		line, isPrefix, err = r.ReadLine()

	}

	return iamNode, neighbours, isInitiator

}

/*

// We create the function checkNeighbourServer.

}
*/

func ReadyToMessageParent(n []Node, parentID string) bool {

	//fmt.Println("######################################")

	//this function checks to see if we have receievd
	//messages from all of our children

	for i := 0; i < len(n); i++ {

		if (n[i].nodeId != parentID) && !n[i].messageReceived {
			//fmt.Println(n[i].nodeId + "not ready")
			//if we find onechild from whom we have not received a
			// message return false
			return false
		}
	}

	//fmt.Println("ready")
	//otherwise return true
	return true

}

func RecordMessageReceived(n []Node, idOfSender string) []Node {

	//fmt.Println("recording message")

	for i := 0; i < len(n); i++ {

		if n[i].nodeId == idOfSender {
			n[i].messageReceived = true
			//fmt.Println(n[i].nodeId + "recorded")

			//if we find onechild from whom we have not received a
			// message return false
		}
	}
	//otherwise return true
	return n

}

func RecordMessagePurge(n []Node, idOfSender string) []Node {

	//fmt.Println("recording message")

	for i := 0; i < len(n); i++ {

		if n[i].nodeId == idOfSender {
			n[i].messageReceived = false
			//fmt.Println(n[i].nodeId + "purged")

			//if we find onechild from whom we have not received a
			// message return false
		}
	}
	//otherwise return true
	return n

}

func RecordMessagePurgeAll(n []Node) []Node {

	//fmt.Println("recording message")

	for i := 0; i < len(n); i++ {

		n[i].messageReceived = false
		//fmt.Println(n[i].nodeId + "purged")

		//if we find onechild from whom we have not received a
		// message return false
	}
	//otherwise return true
	return n

}

func AlgObjToString(inputobj AlgorithmDataObj) string {

	var outputString string

	outputString = strconv.Itoa(inputobj.n) + "&&" + strconv.Itoa(inputobj.id) + "&&" + strconv.Itoa(inputobj.thirdParameter) + "&&" + inputobj.idOfSender + "\n"

	return outputString
}

func StringToAlgObj(messageString string) AlgorithmDataObj {

	var outputObj AlgorithmDataObj

	messageString = strings.TrimSuffix(messageString, "\n")

	//outputString = strconv.Itoa(inputobj.n) + "&&" + strconv.Itoa(inputobj.id) + "&&" + strconv.Itoa(inputobj.thirdParameter) + "&&" + inputobj.idOfSender

	temp := strings.Split(messageString, "&&")

	i0, _ := strconv.Atoi(temp[0])
	i1, _ := strconv.Atoi(temp[1])
	i2, _ := strconv.Atoi(temp[2])

	outputObj = AlgorithmDataObj{i0, i1, i2, temp[3]}

	return outputObj
}
