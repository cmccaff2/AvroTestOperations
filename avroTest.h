//
//  avroTest.h
//  Avro4
//
//  Created by Connor McCaffrey on 2022-06-01.
//

#ifndef avroTest_h
#define avroTest_h

#include <fstream>

#include "avro/Encoder.hh"
#include "avro/Decoder.hh"
#include "avro/ValidSchema.hh"
#include "avro/Compiler.hh"
#include "avro/Generic.hh"
#include "avro/DataFile.hh"

namespace avroTest{

//*********Lower level tests using streams and encoders/decoders directly**************//


// Test function for encoding a single object and sending it directly to a file.
// outFileName is overwritten if it exists
template <typename T>
void objToFile(const T& toEncode, const std::string& outFileName)
{
    // Output streams always use unique_ptrs. Also typeDef'ed to OutputStreamPtr
    // Use avro::memoryOutputStream to write to a temporary memory buffer instead
    std::unique_ptr<avro::OutputStream> out = avro::fileOutputStream(outFileName.c_str());
    avro::EncoderPtr e = avro::binaryEncoder();
    e->init(*out); // Future encode operations are sent to out

    avro::encode(*e, toEncode); // Write encoded object to stream
    out->flush(); //Ensure contents of stream are sent to the file
}

// Test function for decoding a single object from a file.
// The decoded object is stored in decodeTo
template <typename T>
void objFromFile(T& decodeTo, const std::string& inFileName)
{
    // Input streams always use unique_ptrs
    // Use avro::memoryInputStream to read from a temporary memory buffer instead
    std::unique_ptr<avro::InputStream> in = avro::fileInputStream(inFileName.c_str());
    avro::DecoderPtr d = avro::binaryDecoder();
    d->init(*in); // decoder d will source data from in
 
    avro::decode(*d, decodeTo); // Decode object and sent to decodeTo
}

// Encodes multiple predefined values to a file without using a schema
// Data is decoded by decodeMultiple function
void encodeMultiple(const std::string& outFileName)
{
    std::unique_ptr<avro::OutputStream> out = avro::fileOutputStream(outFileName.c_str());
    avro::EncoderPtr e = avro::binaryEncoder();
    e->init(*out); // Future encode operations are sent to out

    // Encode values, send to stream
    e->encodeString("Test");
    e->encodeBool(true);
    e->encodeInt(2);
    e->encodeInt(256);
    e->encodeFloat(0.f);
    out->flush();
    
    // outFile contents are: 08 54 65 73 74 01 04 80 04 00 00 00 00
    // string=(08 54 65 73 74) bool=(01) int=(04) int=(80 04) float=(00 00 00 00)
    // note the variable length of ints and longs
}

// Decodes the data written by encodeMultiple
// Requires prior knowledge of data since no schema is used
void decodeMultiple(const std::string& inFileName)
{
    std::unique_ptr<avro::InputStream> in = avro::fileInputStream(inFileName.c_str());
    avro::DecoderPtr d = avro::binaryDecoder();
    d->init(*in);
    
    std::cout << "Decoding from " << inFileName << ":" << std::endl;
    //std::cout << d->decodeString() << std::endl;
    // We can choose to skip strings, byte arrays, fixed byte arrays
    // and attempt to skip maps and arrays (which only succeeds if the elements themselves are skippable
    d->skipString();
    std::cout << d->decodeBool() << std::endl;
    std::cout << d->decodeInt() << std::endl;
    std::cout << d->decodeInt() << std::endl;
    std::cout << d->decodeFloat() << std::endl;
}

// Reads len bytes from inFileName and returns a pointer to the data
const u_int8_t* readBytes(const std::string& inFileName, size_t len)
{
    std::unique_ptr<avro::InputStream> in = avro::fileInputStream(inFileName.c_str());
    const u_int8_t* data;
    in->next(&data, &len); // Get the next len bytes and store in data
    
    return data;
    
}

// Decodes an array of bytes
// Assumes the data is in the format written by encodeMultiple
void decodeBytes(const u_int8_t* bytes)
{
    // Construct an input stream referring to memory rather than a file
    // bytes are not copied
    std::unique_ptr<avro::InputStream> in = avro::memoryInputStream(bytes, 13);
    
    avro::DecoderPtr d = avro::binaryDecoder();
    d->init(*in);

    std::cout << "Decoding from memory: \n";
    std::cout << d->decodeString() << std::endl;
    std::cout << d->decodeBool() << std::endl;
    std::cout << d->decodeInt() << std::endl;
    std::cout << d->decodeInt() << std::endl;
    std::cout << d->decodeFloat() << std::endl;
}

// Tests skipping and seeking of data written by encodeMultiple using a SeekableInputStream
void streamSeek(const std::string& inFileName)
{
    // Only seems to support seekable file streams instead of memory streams
    avro::SeekableInputStreamPtr in = avro::fileSeekableInputStream(inFileName.c_str());
    
    avro::DecoderPtr d = avro::binaryDecoder();
    d->init(*in);
    
    std::cout << "Seekable Decoding: \n";
    d->skipString();
    std::cout << d->decodeBool() << std::endl;
    std::cout << d->decodeInt() << std::endl;
    std::cout << d->decodeInt() << std::endl;
    d->init(*in); // Seems like we must re-init the decoder THEN seek the stream
    in->seek(0);

    std::cout << d->decodeString() << std::endl;
}


// Compiles a schema from a JSON file containing the schema alone
avro::ValidSchema compileSchema(const std::string& inFileName)
{
    std::ifstream ifs(inFileName);
    avro::ValidSchema result;
    avro::compileJsonSchema(ifs, result);
    return result;
}

// Read a binary file containing data as defined by the given schema
// File should not contain a header
void parseBinaryFile(avro::ValidSchema schema, const std::string& inFileName)
{
    std::unique_ptr<avro::InputStream> in = avro::fileInputStream(inFileName.c_str());
    avro::DecoderPtr d = avro::binaryDecoder();
    d->init(*in);
 
    std::cout << "Parsing binary file\n";

    avro::GenericDatum datum(schema); // Type that can hold any avro type (including records, arrays, etc). Specifics determined by schema
    avro::decode(*d, datum); // Read/decode an item in full using the schema
    
    std::cout << "Type: " << datum.type() << std::endl;
    if(datum.type() == avro::AVRO_RECORD) // Need to check the type for handling
    {
        avro::GenericRecord& rec = datum.value<avro::GenericRecord>(); // Retrieve the record
        
        for (int i = 0; i < rec.fieldCount(); i++) { // Iterate though the fields
            avro::GenericDatum& field = rec.fieldAt(i); // Retrieve the field
            std::cout << field.type();
            
            if(field.type() == avro::AVRO_STRING)
            {
                std::cout << ": " << field.value<std::string>();
            }
            std::cout << std::endl;

        }
    }
}

//******** Tests dealing with full Avro datafiles ********//
    
// Create an avro file with a header containing metadata including the schema
// numEntries is the number of sample records to add to filename
void createDataFile(const std::string& filename, avro::ValidSchema schema, int numEntries)
{
    // Writer handles header creation, compression (if requested), encoding, creating sync markers
    avro::DataFileWriter<avro::GenericDatum> dataWriter(filename.c_str(), schema);
    
    avro::GenericDatum datum(schema); // Object that can contain any avro type

    for (int i = 0; i < numEntries; i++) { // Add the specified number of entries to the file
        
        if (datum.type() == avro::AVRO_RECORD) {
            avro::GenericRecord& rec = datum.value<avro::GenericRecord>(); // First obtain the record stored in the datum
            
            // Need to set each field of the record
            for (int fieldNum = 0; fieldNum < rec.fieldCount(); fieldNum++) { // Iterate through fields
                avro::GenericDatum& field = rec.fieldAt(fieldNum); // Get reference to field
                
                // Set fields with sample values
                switch (field.type()) {
                    case avro::AVRO_STRING:
                        field.value<std::string>() = "Test";
                        break;
                    case avro::AVRO_BOOL:
                        field.value<bool>() = true;
                        break;
                    case avro::AVRO_INT:
                        field.value<int>() = i;
                        break;
                    case avro::AVRO_FLOAT:
                        field.value<float>() = float(i);
                        break;
                    default:
                        // other types are possible (arrays, longs, etc)
                        break;
                }
            } // Field for loop
        } // if AVRO_RECORD
        else if(datum.type() == avro::AVRO_INT)
        {
            datum.value<int>() = i;
        }else if(datum.type() == avro::AVRO_FLOAT)
        {
            // ...
        }
        // and other types (string, bool, array, map, etc)
        
        // Values set, write datum to file
        dataWriter.write(datum);
    }
    
    dataWriter.flush();
    dataWriter.close();
}

void readDataFile(const std::string& fileName)
{
    // Reader handles interpreting of header, decompression, decoding
    // Can pass a schema or rely on schema in file headers
    avro::DataFileReader<avro::GenericDatum> dataReader(fileName.c_str());

    avro::GenericDatum datum(dataReader.dataSchema());
    while (dataReader.read(datum)) // Read all entries
    {
        if(datum.type() == avro::AVRO_RECORD)
        {
            avro::GenericRecord& rec = datum.value<avro::GenericRecord>();
            
            for (int fieldNum = 0; fieldNum < rec.fieldCount(); fieldNum++) { // Iterate through fields
                avro::GenericDatum& field = rec.fieldAt(fieldNum); // Get reference to field
                
                // Get field values
                switch (field.type()) {
                    case avro::AVRO_INT: {
                        int i = field.value<int>();
                        break;
                    }
                    default: {
                        // other types are possible (arrays, longs, etc)
                        break;
                    }
                }
            } // Field for loop

        }
    }
    dataReader.close();
}



};


#endif /* avroTest_h */
