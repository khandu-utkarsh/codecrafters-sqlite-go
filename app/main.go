package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

func interpretBytes(serialType int64, raw []byte) (int64, interface{}) {	
	switch serialType {
	case 0:
		return 0, nil
	case 1:
		val8 := int8(raw[0]);
		return 1, int64(val8);
	case 2:
		val16 := int16(binary.BigEndian.Uint16(raw[:2]))
		return 2, int64(val16);
	case 3:
		value32:= int32(raw[0]) << 16 | int32(raw[1]) << 8 | int32(raw[2]);
		if value32 & 0x800000 != 0 {
			value32 |= -1 << 24
			// value32 |= 0xFF000000
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
			// val48 |= uint64(0xFFFF000000000000)
			val48 |= -1 << 48
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


//!Record belongs to one row from the table containing all the cols
func parseRecord(recordRaw []byte) ([]int64, []interface{}) {
	//!Record is basically each row of the table or index. 
	totalBytesCountHeader, hSize := binary.Varint(recordRaw);

	var currOffset int64;
	currOffset = int64(hSize);
	
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
	return colsSerialTypes, colContents
}

//!Assuming it is cell of type ==> Table B-Tree Leaf Cell:
//!Also assumption is that there is no overflow
func readCell(pageBytes []byte, cellOffset uint16) ([]int64, []interface{}) {
	payloadSizeInBytes, sizeBytesRead := binary.Varint(pageBytes[cellOffset : cellOffset + 9]);
	currOffset := cellOffset + uint16(sizeBytesRead);
	_, rowIdBytesRead := binary.Varint(pageBytes[currOffset : currOffset + 9]);
	currOffset += uint16(rowIdBytesRead);
	currCellPayloadBytes := pageBytes[int64(currOffset) : int64(currOffset) + payloadSizeInBytes];

	//!Parse this record
	cellColsSerialType, cellColsContent := parseRecord(currCellPayloadBytes);
	return cellColsSerialType, cellColsContent
}


//!Since offset on each page are from beginning, so it is better to read the whole page and continue.
func getCellPointersOnPage(pageBytes []byte, pageSize int64, firstPage bool) []uint16 {
	//!If first page, there will be file header, else page won't have file header, it will directly have page header
	//!If it is the first page, page size will be 100 bytes less which has removed the header.

	var fileHeaderOffset int64
	fileHeaderOffset = 0;
	if(firstPage) {
		fileHeaderOffset = 100;
	}
	
	//!Get the size of page header, which is first byte after file header (and first byte on page if no page header)
	var pageHeaderSizeInBytes int64
	if pageBytes[fileHeaderOffset] == 0x05 || pageBytes[fileHeaderOffset] == 0x02 {	//!Interior table page.
		pageHeaderSizeInBytes = 12
	}	else {
		pageHeaderSizeInBytes = 8
	}

	currOffset := fileHeaderOffset;

	//!Cells count on page
	cellsCount := int64(binary.BigEndian.Uint16(pageBytes[currOffset + 3 : currOffset + 5]))

	currOffset += currOffset + pageHeaderSizeInBytes;

	//!Get pointers to all the cells.
	cellPointers := make([]uint16, cellsCount);
	for i := int64(0); i < cellsCount; i++ { 		//!2 bytes is the cell size
		cellPointers[i] = binary.BigEndian.Uint16(pageBytes[currOffset + 2 * i : currOffset + 2 * (i + 1)])
    }
	return cellPointers;
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

		pageBytes := make([]byte, int64(pageSize));
		databaseFile.ReadAt(pageBytes, 0);

		//!First page will have sql_schema table and need to go through each row having info of the table and extract table name from it.
		cellPointers := getCellPointersOnPage(pageBytes, int64(pageSize), true);

		var names []string;

		// Iterate using range
		for _, cellPointer := range cellPointers {
			//cellColsSerialType, cellColsContent := readCell(pageBytes, cellPointer);
			_, cellColsContent := readCell(pageBytes, cellPointer);

			//serialTypeOfCol0 := cellColsSerialType[1];
			stringInCol0 := cellColsContent[1].(string);

			//serialTypeOfCol1 = cellColsSerialType[2];
			stringInCol1 := cellColsContent[2].(string);
			

			//!For debugging
			fmt.Println("Col0 name: ", stringInCol0);
			fmt.Println("Col1 name: ", stringInCol1);

			//!Schema table consists of type, name, tbl_name, ... ...
			names = append(names, stringInCol0);
		}

		for _, name := range names {
			fmt.Println(name);
		}


		databaseFile.Close();



	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
