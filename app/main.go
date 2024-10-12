package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

func parseColNamesFromSQLStr(sql string) []string {
	indStart := strings.Index(sql, "(");
	indEnd := strings.Index(sql, ")");

	colsDetails := sql[indStart + 1 : indEnd];
	splitted := strings.Split(colsDetails, ",");
	var colNames []string;
	for _, col := range splitted {
		col = strings.Trim(col, " ");
		splittedCol := strings.Split(col, " ");
		colNames = append(colNames, splittedCol[0]);
	}
	return colNames;
}

func getRootPageAndCreationStrForTable(tableName string, pageBytes []byte) (int64, string) {
	//!First page will have sql_schema table and need to go through each row having info of the table and extract table name from it.
	cellPointers := getCellPointersOnPage(pageBytes, true);

	var tableRootPage int64;

	pageFound := false;
	var cellColsContent []interface{};
	var sqlStr string;
	// Iterate using range
	for _, cellPointer := range cellPointers {
		_, cellColsContent = readCell(pageBytes, cellPointer);
		//!If the name of the table is equal to the supplied name, extract the root page.
		//!Schema table consists of type, name, tbl_name, ... ...
		nameCol := cellColsContent[1].(string);
		if(nameCol == tableName) {	//!Since it won't be a float or string or blob, hence it will always return int64
			tableRootPage = cellColsContent[3].(int64);
			sqlStr = cellColsContent[4].(string);
			pageFound = true;		
			break;
		}
	}
	if(!pageFound) {
		return int64(-1), sqlStr;
	}
	return tableRootPage, sqlStr;
}


func getPageOffset(pageno int64, pageSize int64) int64 {
	return (pageno - 1) * pageSize;	
}




//!For using the go inbuild function binary.Varint, we need to send least significant numbers first and then the most significant ones.
//!But in out case, we have most significant ones first and then least. So here using the custom method.

//!Not taking care of negative value. In the documentation it is given that take care of negative values as well.
func ReadVarint(buf []byte) (uint64, int) {
	var x uint64
	for i, b := range buf {
		x = (x << 7) | uint64(b & 0x7F)
		if b & 0x80 == 0 {
			return x, i + 1
		}
	}
	return 0, 0
}

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
			value32 |= -1 << 24	//!Since it is a 2 compliments integer, taking care of negative values.
		}
		return 3, int64(value32);
	case 4:
		val32 := int32(binary.BigEndian.Uint32(raw[:4]))
		return 4, int64(val32);
	case 5:
		val48 := int64(raw[0])<<40 | int64(raw[1])<<32 | int64(raw[2])<<24 | int64(raw[3])<<16 | int64(raw[4])<<8 | int64(raw[5])

		// Check if the sign bit (48th bit) is set
		if val48 & 0x800000000000 != 0 {
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
			localString := string(raw[0:serialBytesCount])
			return serialBytesCount, localString;
		}
	}
	return -1, nil // Adding a return statement in case none of the cases match
}

//!Record belongs to one row from the table containing all the cols
func parseRecord(recordRaw []byte) ([]int64, []interface{}) {
	//!Record is basically each row of the table or index.
	totalBytesCountHeader, hSize := ReadVarint(recordRaw);

	var currOffset int64;
	currOffset = int64(hSize);
	
	var colsSerialTypes []int64
	var colContents []interface{}

	contentBytesOffset := int64(totalBytesCountHeader);

	for currOffset < int64(totalBytesCountHeader) {
		serialTypeU, bytesRead := ReadVarint(recordRaw[currOffset : totalBytesCountHeader]);
		serialType := int64(serialTypeU);
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
//!Does go pass value by reference or by value. Look into it.
func readCell(pageBytes []byte, cellOffset uint16) ([]int64, []interface{}) {
	payloadSizeInBytes, sizeBytesRead := ReadVarint(pageBytes[cellOffset : cellOffset + 9]);
	currOffset := cellOffset + uint16(sizeBytesRead);
	_, rowIdBytesRead := ReadVarint(pageBytes[currOffset : currOffset + 9]);

	currOffset += uint16(rowIdBytesRead);
	currCellPayloadBytes := pageBytes[int64(currOffset) : int64(currOffset) + int64(payloadSizeInBytes)];

	//!Parse this record
	cellColsSerialType, cellColsContent := parseRecord(currCellPayloadBytes);

	// //!For debugging
	// for _, b := range cellColsContent {
	// 	fmt.Println(b);
	// }

	return cellColsSerialType, cellColsContent
}


//!Since offset on each page are from beginning, so it is better to read the whole page and continue.
func getCellPointersOnPage(pageBytes []byte, firstPage bool) []uint16 {
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

	currOffset += pageHeaderSizeInBytes;

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
	commandRead := os.Args[2]

	command := commandRead;
	comPrefix := strings.HasPrefix(command, "SELECT") || strings.HasPrefix(command, "select");
	if(comPrefix) {
		command = "SELECT";
	}



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
		cellPointers := getCellPointersOnPage(pageBytes, true);

		var names []string;

		// Iterate using range
		for _, cellPointer := range cellPointers {
			//cellColsSerialType, cellColsContent := readCell(pageBytes, cellPointer);
			_, cellColsContent := readCell(pageBytes, cellPointer);
			//!Schema table consists of type, name, tbl_name, ... ...
			nameCol := cellColsContent[1].(string);
			names = append(names, nameCol);
		}
		for _, name := range names {
			fmt.Println(name);
		}
		databaseFile.Close();
	case "SELECT":		
		splitted := strings.Split(commandRead, " ");

		var selectIndex, fromIndex, whereIndex int;
		selectIndex = -1;
		fromIndex = -1;
		whereIndex = -1;
		for in, col := range splitted {
			if(col == "SELECT" || col == "select") {
				selectIndex = in;
			} else if(col == "FROM" || col == "from") {
				fromIndex = in;				
			} else if(col == "WHERE" || col == "where") {
				whereIndex = in;
			}
		}
		
		_ = whereIndex;

		//fmt.Println(selectIndex, fromIndex, whereIndex);

		colNamesRaw := splitted[selectIndex + 1: fromIndex];
		colNamesInp := make([]string, len(colNamesRaw))

		for i, col := range colNamesRaw {
			colNamesInp[i] = strings.Trim(col, "")
			colNamesInp[i] = strings.Trim(col, ",")
		}
		// fmt.Println(colNamesInp);
		//!Testing;

		tableName := splitted[fromIndex + 1];
		//fmt.Println(tableName)
		

		//!Find the root page of the table
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
		tablePageNo, sqlStr := getRootPageAndCreationStrForTable(tableName, pageBytes);
		//fmt.Println(sqlStr); //!For debugging.
		colNames := parseColNamesFromSQLStr(sqlStr);
		//fmt.Println(colNames);
		var colIndices []int;
		for _, col := range colNamesInp {
			for colIndex, colNameCreation := range colNames {
				if(colNameCreation == col) {
					colIndices = append(colIndices, colIndex);
					break;
				}
			}
		}

		//fmt.Println(colIndices);

		giveRowsCount := false;
		if(len(colIndices) == 0) {
			giveRowsCount = true;
		}

		//!Now going to the table page, and interpretting values:
		tablePageOffset := getPageOffset(tablePageNo, int64(pageSize));
		tablePageBytes := make([]byte, int64(pageSize));
		databaseFile.ReadAt(tablePageBytes, tablePageOffset);

		//!All the rows on the page.
		cellPointers := getCellPointersOnPage(tablePageBytes, false);		
		if(giveRowsCount) {
			fmt.Println((len(cellPointers)));
		} else {		
			//!Iterating over each roww (record)
			for _, cellPointer := range cellPointers {
				//colSerialTypes, cellColsContent := readCell(tablePageBytes, cellPointer);
				_, cellColsContent := readCell(tablePageBytes, cellPointer);
				var outString string;
				for i, colIndex := range colIndices {
					if(i != 0) {
						outString += "|";
					}
					//reqColSerialType := colSerialTypes[colIndex];
					//decodeInto = reqColSerialType;
					reqColContent :=  (cellColsContent[colIndex]).(string);
					outString += reqColContent;
					//!Asumming everything to be string for simplicity
					// if (decodeInto == 7) {
					// 	curr := row.(float64);
					// 	fmt.Println(curr);
					// 	//!Req content is float
					// } else if(decodeInto%2 == 0 && decodeInto >= 12) {
					// 	curr := row.([]byte)
					// 	fmt.Println(curr);
					// } else if(decodeInto%2 != 0 && decodeInto > 13) {
					// 	//!String	
					// 	curr := row.(string);
					// 	fmt.Println(curr);
					// 	} else {
					// 	//!Int 64	
					// 	curr := row.(int64);
					// 	fmt.Println(curr);
					// }
				}
				fmt.Println(outString);
			}
		}
		databaseFile.Close();
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
