//
//  main.cpp
//  Avro4
//
//  Created by Connor McCaffrey on 2022-05-31.
//
#include <iostream>
#include <fstream>

#include "cpx.hh"
#include "avroTest.h"

// Perform a series of Avro operations ranging from low-level encoding/decoding to high-level data file handling
int main(int argc, const char * argv[]) {
    // Directly write binary encoding of c1 to file
    // object must have an encode/decode function as seen in cpx.h
    c::cpx c1;
    c1.re = 1;
    c1.im = 2;
    avroTest::objToFile<c::cpx>(c1, "cpx.b");
    
    // Read binary encoding of c2 from file
    c::cpx c2;
    avroTest::objFromFile(c2, "cpx.b");
    std::cout << '(' << c2.re << ", " << c2.im << ')' << std::endl;
    
    avroTest::encodeMultiple("multi.b"); // Encode and write multiple datatypes without header/schema
    avroTest::decodeMultiple("multi.b"); // Read and decode values written by encodeMultiple
    
    // Reads the 13 bytes written to "multi.b"
    const u_int8_t* bytes = avroTest::readBytes("multi.b", 13);
    avroTest::decodeBytes(bytes); // Decodes the data first written by encodeMultiple

    //Skips the string, reads other values, then seeks back to start and reads string
    avroTest::streamSeek("multi.b");
    
    // Create a schema of the data in multi.b
    std::string schema ="{\"type\": \"record\", \"name\": \"multi\", \"fields\" : [{\"name\": \"string\", \"type\": \"string\"},{\"name\": \"bool\", \"type\" : \"boolean\"},{\"name\": \"int1\", \"type\": \"int\"},{\"name\": \"int2\", \"type\": \"int\"},{\"name\": \"float\", \"type\": \"float\"}]}";

    // Save schema to file
    std::ofstream out ("multi.json", std::ofstream::out);
    out << schema;
    out.close();

    // Compile our schema into a validSchema object
    avro::ValidSchema multiSchema = avroTest::compileSchema("multi.json");
    avroTest::parseBinaryFile(multiSchema, "multi.b"); // Use the schema to read the file
    
    // Create full avro data file with header including schema
    avroTest::createGenericDataFile("multi.avro", multiSchema, 100);
    
    avroTest::readGenericDataFile("multi.avro");
    
    avroTest::writeCPXFile("cpx.avro", 50);
    avroTest::readCPXFile("cpx.avro");
    return 0;
}
