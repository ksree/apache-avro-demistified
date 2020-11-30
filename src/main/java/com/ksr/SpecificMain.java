package com.ksr;

import com.ksr.avro.Department;
import com.ksr.avro.Employee;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

public class SpecificMain {
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

    }
}
