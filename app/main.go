package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"math"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

func interpretBytes(serialType int64, raw []byte) (int64, interface{}) {	
	switch serialType {
	case 0:
		return 0, null
	case 1:
		val8 := int8(raw[0]);
		return 1, int64(val8);
	case 2:
		val16 := int16(binary.BigEndian.Uint16(raw[:2]))
		return 2, int64(val16);
	case 3:
		value32:= int32(raw[0]) << 16 | int32(raw[1]) << 8 | int32(raw[2]);
		if value32 & 0x800000 != 0 {
			value32 |= 0xFF000000
		}
		return 3, int64(value32);
	case 4:
		val32 := int32(binary.BigEndian.Uint32(raw[:4]))
		return 4, int64(val32);
	case 5:
		val48 := int64(raw[0])<<40 | int64(raw[1])<<32 | int64(raw[2])<<24 | int64(raw[3])<<16 | int64(raw[4])<<8 | int64(raw[5])

		// Check if the sign bit (48th bit) is set
		if val48 & 0x800000000000 != 0 {
			// Extend the sign to 64 bits if the 48th bit is set
			val48 |= 0xFFFF000000000000
		}
		return 6, val48;
	case 6:
		val64 := int64(binary.BigEndian.Uint64(raw[:8]))
		return 8, int64(val64);
	case 7:
		floatVal64 := math.Float64frombits(binary.BigEndian.Uint64(raw[:8]));
		return 8, floatVal64;
	case 8:
		return 0, 0
	case 9:
		return 0, 0
	default:
		if serialType%2 == 0 && serialType >= 12 {
			serialBytesCount := (serialType - 12) / 2
			return serialBytesCount, raw[0:serialBytesCount];
		} else if serialType%2 != 0 && serialType > 13 {
			serialBytesCount := (serialType - 13) / 2
			return serialBytesCount, string(raw[0:serialBytesCount]);
		}
	}
	return -1, nil // Add a return statement in case none of the cases match
}



func parseRecord(recordRaw []byte, headerSize int64) {

	//!Record is basically each row of the table or index. 
	totalBytesCountHeader, hSize := binary.Varint(recordRaw);

	var currOffset int64;
	currOffset = int64(hSize);

	colIndex := 0;

	var colsSerialTypes []int64
	var colContents []interface{}

	contentBytesOffset := totalBytesCountHeader;

	for currOffset < totalBytesCountHeader {
		serialType, bytesRead := binary.Varint(recordRaw[currOffset : ]);
		bytesReadForContent, contentValue := interpretBytes(serialType, recordRaw[contentBytesOffset:])	
		currOffset += int64(bytesRead);
		contentBytesOffset += int64(bytesReadForContent);
		colsSerialTypes = append(colsSerialTypes, serialType);
		colContents = append(colContents, contentValue);
		
	}

}


func parsePage(rawBytes []byte, pageSize int, firstPage bool) {
	//!Since cell pointers are from beginning of the page, it is better to send whole page.


	//!If it is the first page, page size will be 100 bytes less which has removed the header.
	var pageHeaderIndex int
	if(firstPage) {
		pageHeaderIndex = 100;
	} else {
		pageHeaderIndex = 0;
	}

	var pageHeaderSize int
	if rawBytes[pageHeaderIndex] == 0x05 || rawBytes[pageHeaderIndex] == 0x02 {	//!Interior table page.
		pageHeaderSize = 12;
	}	else {
		pageHeaderSize = 8;
	}

	//!Next step is to get the cell count on the page.
	var cellCount uint16;
	cellCountBytes := bytes.NewReader(rawBytes[pageHeaderIndex + 3: pageHeaderIndex + 5]);
	binary.Read(cellCountBytes, binary.BigEndian, &cellCount);

	k = int(cellCount);

	cellPointers := make([]uint16, k);

	cellPointerArrayOffset := pageHeaderIndex + pageHeaderSize;

	//!Now read cell pointer array and store them.
	for i := 0; i < k; i++ {
		currentPointerBytesReader := bytes.NewReader(rawBytes[cellPointerArrayOffset + (k *2) : cellPointerArrayOffset + (k + 1)* 2]);
		binary.Read(currentPointerBytesReader, binary.BigEndian, &(cellPointers[i]));
    }

	for i := 0; i <k; i++ {
		currCellPointer := cellPointers[i];
		//!total bytes of playload
		//!Since max size of varint would be 9 bytes, we can pass only 9 bytes for interpretation.

		payloadSize, sizeBytesRead := binary.Varint(rawBytes[currCellPointer : currCellPointer + 9]);
		_, rowIdBytesRead := binary.Varint(rawBytes[currCellPointer +uint16(sizeBytesRead) : currCellPointer + uint16(sizeBytesRead) + 9]);

		//!byte array of payload
		//!This will be the record format:		
		currCellPayload := rawBytes[uint64(currCellPointer) +uint64(sizeBytesRead)+uint64(rowIdBytesRead) : uint64(currCellPointer)+uint64(sizeBytesRead)+uint64(rowIdBytesRead)+uint64(payloadSize)]
// fmt.Printf("payload: %v\n", string(record))



	}

	//!There cell pointers are basically offset from top of the page. From 0
	
	//!Note:
	//!Also harding coding it to be Table B-Tree Leaf Cell 
	//!Also, no overflow will happen, so no need to worry about it.





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
