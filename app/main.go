package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

func parsePage(rawBytes []byte, pageSize int, firstPage bool) {
	//!If it is the first page, page size will be 100 bytes less which has removed the header.

	var pageHeaderSize int
	if rawBytes[0] == 0x05 || rawBytes[0] == 0x02 {	//!Interior table page.
		pageHeaderSize = 12;
	}	else {
		pageHeaderSize = 8;
	}

	//!Next step is to get the cell count on the page.
	var cellCount uint16;
	cellCountBytes := bytes.NewReader(rawBytes[3:5]);
	binary.Read(cellCountBytes, binary.BigEndian, &cellCount);

	k = int(cellCount);

	cellPointers := make([]uint16, k);

	//!Now read cell pointer array and store them.
	for i := 0; i < k; i++ {
		currentPointerBytesReader := bytes.NewReader(rawBytes[pageHeaderSize + (k *2) : (k + 1)* 2]);
		binary.Read(currentPointerBytesReader, binary.BigEndian, &(cellPointers[i]));
    }
	
	//!At this point we have gotten pointer to every cell on this page. --> Next step would be to go to these cells and get the name







}


// InterpretPageHeader interprets the B-tree page type from a single byte.
func GetCellContentStartOffset(i byte) int {
	switch i {
	case 0x02:	//!This is interior b-tree page.
		return 12;
	default:	//!All rest headers are of size 8
		return 8;
	}
}

func ReadLeafTableBTreePage()
{

}


// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		header := make([]byte, 100)

		_, err = databaseFile.Read(header)
		if err != nil {
			log.Fatal(err)
		}

		var pageSize uint16
		if err := binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &pageSize); err != nil {
			fmt.Println("Failed to read integer:", err)
			return
		}
		// You can use print statements as follows for debugging, they'll be visible when running tests.
		//fmt.Println("Logs from your program will appear here!")

		// Uncomment this to pass the first stage
		fmt.Printf("database page size: %v\n", pageSize)

		//!By default it is page 1 is at offset zero and contains sqlite_schema.
		//!var default offset with the size 100

		pageHeader := make([]byte, 12)
		_, err = databaseFile.Read(pageHeader)
		if err != nil {
			log.Fatal(err)
		}
		
		var cellsCount uint16;
		if err := binary.Read(bytes.NewReader(pageHeader[3:5]), binary.BigEndian, &cellsCount); err != nil {
			fmt.Println("Failed to get cell count:", err)
			return
		}

		intCellCount := int(cellsCount);
		// Logging the cell count, which is same as tables count in this case, since we don't have other things like index, views, triggers etc.
		fmt.Printf("number of tables: %v", intCellCount)


		databaseFile.Close();

	case ".tables":

		//!Return the name of tables present in the db file.

		//!Steps:
		//!Open the DB file. Then go to the page header section.


		// databaseFilePath := os.Args[1]
		// command := os.Args[2]
	
		// switch command {
		// case ".dbinfo":
		// 	databaseFile, err := os.Open(databaseFilePath)
		// 	if err != nil {
		// 		log.Fatal(err)
		// 	}
	
		// 	header := make([]byte, 100)
	
		// 	_, err = databaseFile.Read(header)
		// 	if err != nil {
		// 		log.Fatal(err)
		// 	}
	
		// 	var pageSize uint16
		// 	if err := binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &pageSize); err != nil {
		// 		fmt.Println("Failed to read integer:", err)
		// 		return
		// 	}
		// 	// You can use print statements as follows for debugging, they'll be visible when running tests.
		// 	//fmt.Println("Logs from your program will appear here!")
	
		// 	// Uncomment this to pass the first stage
		// 	fmt.Printf("database page size: %v\n", pageSize)
	
		// 	//!By default it is page 1 is at offset zero and contains sqlite_schema.
		// 	//!var default offset with the size 100
	
		// 	pageHeader := make([]byte, 12)
		// 	_, err = databaseFile.Read(pageHeader)
		// 	if err != nil {
		// 		log.Fatal(err)
		// 	}
			
		// 	var cellsCount uint16;
		// 	if err := binary.Read(bytes.NewReader(pageHeader[3:5]), binary.BigEndian, &cellsCount); err != nil {
		// 		fmt.Println("Failed to get cell count:", err)
		// 		return
		// 	}
	
		// 	intCellCount := int(cellsCount);
		// 	// Logging the cell count, which is same as tables count in this case, since we don't have other things like index, views, triggers etc.
		// 	fmt.Printf("number of tables: %v", intCellCount)
	
	
		// 	databaseFile.Close();
	



		//!Read the page 1 --> Because sql lite schema will be on that page.
		//!Go to cell pointer array to get the offset of each cell
		//!Go to each cell and there go to payload place. 



	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
