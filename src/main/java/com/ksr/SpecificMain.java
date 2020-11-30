package com.ksr;

import com.ksr.avro.Department;
import com.ksr.avro.Employee;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {

        //Serializing and deserializing with code gen
        Department dept = new Department();
        dept.setId(99);
        dept.setName("Accounts");

        Employee emp1 = new Employee();
        emp1.setId(1);
        emp1.setFirst("Nikhil");
        emp1.setLast("Chinnappa");
        emp1.setDepartment(dept);

        // Alternate constructor
        Employee emp2 = new Employee(2, "Rick", "Ross", "RR", 27, dept);

        //Construct via builder
        Employee emp3 = Employee.newBuilder()
                .setId(3)
                .setFirst("Sunil")
                .setLast("Gav")
                .setAlias("gav")
                .setAge(55)
                .setDepartment(dept)
                .build();

        File employeeData = new File("target/generated-sources/employee.avro");
        //Serialize employees and create an avro data file
        DatumWriter<Employee> empDatumWriter = new SpecificDatumWriter<Employee>(Employee.class);
        DataFileWriter<Employee> dataFileWriter = new DataFileWriter<Employee>(empDatumWriter);
        dataFileWriter.create(emp1.getSchema(), employeeData);
        dataFileWriter.append(emp1);
        dataFileWriter.append(emp2);
        dataFileWriter.append(emp3);
        dataFileWriter.close();

        //Deserialize the above generated avro data file
        DatumReader<Employee> userDatumReader = new SpecificDatumReader<Employee>(Employee.class);
        DataFileReader<Employee> dataFileReader = new DataFileReader<Employee>(employeeData, userDatumReader);
        Employee emp = null;
        while (dataFileReader.hasNext()) {
            emp = dataFileReader.next(emp);
            System.out.println(emp);
        }
        //Serializing and deserializing without code generation. Using Schema parsers
        Schema schema = new Schema.Parser().parse(employeeData);
        GenericRecord empl1 = new GenericData.Record(schema);
        empl1.put("id", 1);
        empl1.put("first", "Nikhil");
        empl1.put("last", "Chinnappa");
        empl1.put("department", dept);

        GenericRecord empl2 = new GenericData.Record(schema);
        empl2.put("id", 2);
        empl2.put("first", "Rick");
        empl2.put("last", "Ross");
        empl2.put("department", dept);

        GenericRecord empl3 = new GenericData.Record(schema);
        empl3.put("id", 3);
        empl3.put("first", "Sunil");
        empl3.put("last", "Gav");
        empl3.put("department", dept);

        File employeeDataNonGen = new File("target/generated-sources/employee_nongen.avro");
        //Serialize
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter1 = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter1.create(schema, employeeDataNonGen);
        dataFileWriter1.append(empl1);
        dataFileWriter1.append(empl2);
        dataFileWriter1.append(empl3);
        dataFileWriter1.close();

        // Deserialize employees from disk
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader1 = new DataFileReader<GenericRecord>(employeeDataNonGen, datumReader);
        GenericRecord emp_1 = null;
        while (dataFileReader.hasNext()) {
            emp_1 = dataFileReader1.next(emp_1);
            System.out.println(emp_1);
        }
    }
}
