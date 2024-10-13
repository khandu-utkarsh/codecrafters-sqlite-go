package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"regexp"
	"strings"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

// ParsedSQL holds the extracted data from the SQL query.
type ParsedSQLQuery struct {
	Columns   []string
	Table     string
	Condition string
}

// parseSQL extracts columns, table, and where condition from an SQL query.
func parseSQL(query string) ParsedSQLQuery {
	// Use case-insensitive regex patterns.
	selectRegex := regexp.MustCompile(`(?i)\bSELECT\s+(.+?)\s+\bFROM\b`)
	fromRegex := regexp.MustCompile(`(?i)\bFROM\s+(\w+)\b`)
	whereRegex := regexp.MustCompile(`(?i)\bWHERE\s+(.+)`)

	// Match the query against each regex pattern.
	columnsMatch := selectRegex.FindStringSubmatch(query)
	tableMatch := fromRegex.FindStringSubmatch(query)
	whereMatch := whereRegex.FindStringSubmatch(query)

	// Extract and clean the columns.
	var columns []string
	if len(columnsMatch) > 1 {
		columns = strings.Split(columnsMatch[1], ",")
		for i := range columns {
			columns[i] = strings.TrimSpace(columns[i])
		}
	}

	// Extract the table name.
	table := ""
	if len(tableMatch) > 1 {
		table = tableMatch[1]
	}

	// Extract the WHERE condition.
	condition := ""
	if len(whereMatch) > 1 {
		condition = strings.TrimSpace(whereMatch[1])
	}

	return ParsedSQLQuery{
		Columns:   columns,
		Table:     table,
		Condition: condition,
	}
}

// func getTableDetailsFromSQLSchemaTable(sql string) (string, []string, []string){
// 	// Regex to extract table name and columns.
// 	re := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(\w+)\s*\(\s*([^;]*)\s*\)`);
// 	match := re.FindStringSubmatch(sql)
// 	if len(match) < 3 {
// 		log.Fatal("Failed to parse the SQL statement.")
// 	}

// 	// Extract table name and columns.
// 	tableName := match[1]
// 	columnsStr := match[2]

// 	// Split columns by commas and trim spaces.
// 	columns := strings.Split(columnsStr, ",")

// 	// fmt.Printf("Table Name: %s\n", tableName)
// 	// fmt.Println("Columns:")

// 	// Iterate over columns and print them.
// 	var colNames []string
// 	var colContentTypes []string	
// 	for _, col := range columns {
// 		col = strings.TrimSpace(col) // Trim spaces around each column.
// 		parts := strings.Fields(col) // Split column name and type.

// 		if len(parts) >= 2 {
// 			columnName := parts[0]
// 			dataType := parts[1]
// 			colNames = append(colNames, columnName);
// 			colContentTypes = append(colContentTypes, dataType);
// 		}
// 	}

// 	return tableName, colNames, colContentTypes;
// }


// Function to extract table details from a SQL schema string.
func getTableDetailsFromSQLSchemaTable(sql string) (string, []string, []string) {
	// Updated regex to handle escaped table names and complex column definitions.
	re := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+"?(\w+)"?\s*\(([^)]+)\)`)
	match := re.FindStringSubmatch(sql)
	if len(match) < 3 {
		log.Fatal("Failed to parse the SQL statement.")
	}

	// Extract table name and columns string.
	tableName := match[1]
	columnsStr := match[2]

	// Split columns by commas but handle cases where commas appear inside column definitions.
	columns := splitColumnsByComma(columnsStr)

	var colNames []string
	var colContentTypes []string

	// Iterate over columns to extract column names and types.
	for _, col := range columns {
		col = strings.TrimSpace(col) // Trim spaces around the column.
		parts := strings.Fields(col) // Split by spaces into name, type, and constraints.

		if len(parts) >= 2 {
			columnName := parts[0] // First part is the column name.
			dataType := parts[1]   // Second part is the data type.

			// Collect column names and data types.
			colNames = append(colNames, columnName)
			colContentTypes = append(colContentTypes, dataType)
		}
	}

	return tableName, colNames, colContentTypes
}

// Helper function to split columns while handling commas inside definitions.
func splitColumnsByComma(columnsStr string) []string {
	var columns []string
	var currentColumn strings.Builder
	inQuotes := false

	for _, char := range columnsStr {
		switch char {
		case ',':
			if inQuotes {
				currentColumn.WriteRune(char) // Keep comma if inside quotes.
			} else {
				columns = append(columns, currentColumn.String())
				currentColumn.Reset()
			}
		case '"':
			inQuotes = !inQuotes // Toggle the inQuotes flag.
			currentColumn.WriteRune(char)
		default:
			currentColumn.WriteRune(char)
		}
	}

	// Add the last column if there's any leftover.
	if currentColumn.Len() > 0 {
		columns = append(columns, currentColumn.String())
	}

	return columns
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
func readTableLeafCell(pageBytes []byte, cellOffset uint16) ([]int64, []interface{}) {
	payloadSizeInBytes, sizeBytesRead := ReadVarint(pageBytes[cellOffset : cellOffset + 9]);
	currOffset := cellOffset + uint16(sizeBytesRead);
	_, rowIdBytesRead := ReadVarint(pageBytes[currOffset : currOffset + 9]);

	currOffset += uint16(rowIdBytesRead);
	currCellPayloadBytes := pageBytes[int64(currOffset) : int64(currOffset) + int64(payloadSizeInBytes)];

	//!Parse this record
	cellColsSerialType, cellColsContent := parseRecord(currCellPayloadBytes);
	return cellColsSerialType, cellColsContent
}

//!Never call this on first page:
func readTable(databaseFile *os.File, pageSize int64 , firstPage bool, currPageBytes []byte ) ([]int64, [][]interface{}) {
	//!Skip the fileHeader in case of page one.
	var fileHeaderOffset int64
	fileHeaderOffset = 0;
	if(firstPage) {
		fileHeaderOffset = 100;
	}
	currOffset := fileHeaderOffset;
	pageHeaderType := currPageBytes[currOffset]
	cellsCount := int64(binary.BigEndian.Uint16(currPageBytes[currOffset + 3 : currOffset + 5]))

	var rightmostChildPageNo int64;
	var pageHeaderSizeInBytes int64;

	if pageHeaderType == 0x05 {
		pageHeaderSizeInBytes = int64(12)
		rightmostChildPageNo = int64(binary.BigEndian.Uint32(currPageBytes[currOffset + 8: currOffset + 12])) 

	} else if pageHeaderType == 0x0d {
		pageHeaderSizeInBytes = int64(8)	
	} else {
		fmt.Println("Error, not correct type");
	}

	currOffset += pageHeaderSizeInBytes;
	//!Get pointers to all the cells.
	cellPointers := make([]uint16, cellsCount);
	for i := int64(0); i < cellsCount; i++ { 		//!2 bytes is the cell size
		cellPointers[i] = binary.BigEndian.Uint16(currPageBytes[currOffset + 2 * i : currOffset + 2 * (i + 1)])
	}

	//!If leaf, directly fetch the content and return, if not recurse.
	if(pageHeaderType == 0x0d) {
		var colRows [][]interface{};
		var colSerial []int64;
		for _, cellPointer := range cellPointers {
			colSerialTypes, cellColsContent := readTableLeafCell(currPageBytes, cellPointer);
			colRows = append(colRows, cellColsContent);
			colSerial = colSerialTypes	//!Would be and should be same for every row.
		}
		return colSerial, colRows;
	}

	//!Interior thing, get page no of children
	childrenPageNos := make([]int64, cellsCount + 1);		
	for i, cellPointer := range cellPointers {
		pagePtrBytes := currPageBytes[cellPointer: cellPointer + 4];
		childrenPageNos[i] = int64(binary.BigEndian.Uint32(pagePtrBytes));
		//_, rowIdBytesRead := ReadVarint(currPageBytes[cellPointer + 4 : cellPointer + 4 + 9]);	//!Not needed rn!!
	} 	
	childrenPageNos[cellsCount] = rightmostChildPageNo;

	var colRows [][]interface{};
	var colSerial []int64;
	for _, childPageNo := range childrenPageNos {
		tablePageOffset := getPageOffset(childPageNo, pageSize);
		tablePageBytes := make([]byte, int64(pageSize));
		databaseFile.ReadAt(tablePageBytes, tablePageOffset);
		serialTypes, rowsContaingCols := readTable(databaseFile, pageSize, firstPage, tablePageBytes);
		colRows = append(colRows, rowsContaingCols...)
		colSerial = serialTypes;	//!Assuming they are all same.

	}
	return colSerial, colRows;
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
		//!Get databse page size and get the number of tables in database


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

		//!Never call this on first page:
		_, rowsContainingCols := readTable(databaseFile, int64(pageSize) , true, pageBytes)
	
		var names []string;
		for _, row := range rowsContainingCols {
			//!Schema table consists of type, name, tbl_name, ... ...
			nameCol := row[1].(string);
			names = append(names, nameCol);
		}

		for _, name := range names {
			fmt.Println(name);
		}
		databaseFile.Close();
	case "SELECT":		

		//!Processing the input query
	    ps := parseSQL(commandRead);

		Q_tableName := ps.Table;
		whereCondition := ps.Condition;
		_ = whereCondition;

		//!Interpet sql_schema table to get table details:
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

		//!Reading and interpretting sql schema table
		_, rowsContainingCols := readTable(databaseFile, int64(pageSize) , true, pageBytes);

		var qTablePageNo int64;
		var sqlStr string;
		pageFound := false;	

		for _, row := range rowsContainingCols {
			//!Schema table consists of type, name, tbl_name, ... ...
			nameCol := row[1].(string);
			if(nameCol == Q_tableName) {
				qTablePageNo = row[3].(int64);
				sqlStr = row[4].(string);
				pageFound = true;		
				break;
			}
		}
		if(!pageFound) {
			fmt.Println("Error, table not found");
		}

		_, colNames, _ := getTableDetailsFromSQLSchemaTable(sqlStr)

		//!Mapping table colummn names to index
		nameToInt := make(map[string]int)
		for i, cn := range colNames {
			nameToInt[cn] = i;
		}

		//!Go to page of the asked table and fetch information of all the records.
		//!Now going to the table page, and interpretting values:
		tablePageOffset := getPageOffset(qTablePageNo, int64(pageSize));
		tablePageBytes := make([]byte, int64(pageSize));
		databaseFile.ReadAt(tablePageBytes, tablePageOffset);
		
		//!Assuming simple conditions for where without AND and other things.
		var ccns []string;
		if(ps.Condition != "") {
			colAndCond := strings.Split(ps.Condition, "=");

			for _, cc := range colAndCond {
				cc = strings.Fields(cc)[0];
				cc = strings.Trim(cc, "'")
				ccns = append(ccns, cc);
			}	
		}

		firstPage := false;
		if(qTablePageNo == 1) {
			firstPage = true;
		}

		//!Reading and interpretting sql schema table
		_, qTableRows := readTable(databaseFile, int64(pageSize) , firstPage, tablePageBytes);	//!Assuming everything to be string for simplicity
		var keepRows [][]interface{};
		for _, allCols := range qTableRows {
			if(len(ccns) != 0) {
				var colDetail string;
				switch v := allCols[nameToInt[ccns[0]]].(type) {
				case string:
					colDetail = v;
				default:
					continue;
				}
				
				//fmt.Println("temp: ", temp);
				if colDetail == ccns[1]	{
					//!I am assuming everything to be string here. Which it might not be
					keepRows = append(keepRows, allCols);
				}
			} else {
				keepRows = append(keepRows, allCols);				
			}
		}

		//!See if it is only asking for count
		if len(ps.Columns) == 1 && (strings.HasPrefix(ps.Columns[0], "COUNT(") ||  strings.HasPrefix(ps.Columns[0], "count(")){
			fmt.Println(len(keepRows));
		} else {
		//!Extract relevant cols:
			for _, allCols := range keepRows {
				var outString string;
				for jin, col := range ps.Columns {
					if(jin != 0) {
						outString += "|"
					}
					//currSerialType := colSerialType[nameToInt[col]];
					//fmt.Println(currSerialType);
					colContent := allCols[nameToInt[col]].(string);

					outString += colContent;
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
