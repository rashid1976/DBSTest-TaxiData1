/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the Li cense.
 */

package com.scb.edmhdpif.processors;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.util.StopWatch;
import org.datanucleus.store.types.converters.DateLongConverter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import com.td.java.util.*;


import static com.scb.edmhdpif.processors.RowIDAssignment.ROW_ID_ADDED;
import static com.scb.edmhdpif.processors.RowIDAssignment.TOTAL_ROW_COUNT;
import static com.scb.edmhdpif.processors.RowIDAssignment.FROM_CELL;
import static com.scb.edmhdpif.processors.RowIDAssignment.TO_CELL;
import static com.scb.edmhdpif.processors.RowIDAssignment.ROW_ID;



@Tags({"data transformation scb hdpedmif"})
@CapabilityDescription("Prepends a row ID to every row of data in the incoming flow file content")
@SideEffectFree
@SupportsBatching
@ReadsAttributes({@ReadsAttribute(attribute = "tablename", description = "The table that is being processed"),
        @ReadsAttribute(attribute = "expected.column.count", description = "The column count for the table")})
@WritesAttributes({
        @WritesAttribute(attribute = TOTAL_ROW_COUNT, description = "This is count of the total number of rows that " +
                "were processed from the flowfile content"),
        @WritesAttribute(attribute = FROM_CELL, description = "This is from Cell"),
        @WritesAttribute(attribute = TO_CELL, description = "This is To Cell"),
        @WritesAttribute(attribute = ROW_ID, description = "This is ROW ID"),
        @WritesAttribute(attribute = ROW_ID_ADDED, description = "A boolean indicating if the ROWID was added to " +
                "the flow file(s) from emitted from this processor.")})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class RowIdDBSTest extends AbstractProcessor {

    /**
     * Provides a {@link Validator} to ensure that provided value is a valid
     * character.
     */
    public static final Validator CHAR_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            // Allows special, escaped characters as input, which is then un-escaped and converted to a single character.
            // Examples for special characters: \t (or \u0009), \f.
            if (input.length() > 1) {
                input = StringEscapeUtils.unescapeJava(input);
            }
            return new ValidationResult.Builder().subject(subject).input(input)
                    .explanation("Only non-null single characters are supported")
                    .valid(input.length() == 1 && input.charAt(0) != 0).build();
        }
    };
    static final PropertyDescriptor INPUT_DELIMITER = new PropertyDescriptor.Builder()
            .name("input-delimiter")
            .displayName("Input delimiter")
            .description("Delimiter character for input columns in each record")
            .addValidator(CHAR_VALIDATOR)
            .expressionLanguageSupported(true)
            .defaultValue("")
            .build();

    static final PropertyDescriptor GeoFile = new PropertyDescriptor.Builder()
            .name("geo-filename")
            .displayName("geo filename")
            .description("geo filename")
            .expressionLanguageSupported(true)
            .defaultValue("")
            .addValidator(Validator.VALID)
            .build();

    static final PropertyDescriptor PLon = new PropertyDescriptor.Builder()
            .name("PLon")
            .displayName("PLon")
            .description("PLon")
            .expressionLanguageSupported(true)
            .defaultValue("")
            .addValidator(Validator.VALID)
            .build();

    static final PropertyDescriptor DLon = new PropertyDescriptor.Builder()
            .name("DLon")
            .displayName("DLon")
            .description("DLon")
            .expressionLanguageSupported(true)
            .defaultValue("")
            .addValidator(Validator.VALID)
            .build();

    static final PropertyDescriptor PLat = new PropertyDescriptor.Builder()
            .name("PLat")
            .displayName("PLat")
            .description("PLat")
            .expressionLanguageSupported(true)
            .defaultValue("")
            .addValidator(Validator.VALID)
            .build();

    static final PropertyDescriptor DLat = new PropertyDescriptor.Builder()
            .name("DLat")
            .displayName("DLat")
            .description("DLat")
            .expressionLanguageSupported(true)
            .defaultValue("")
            .addValidator(Validator.VALID)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship")
            .build();


    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failure relationship")
            .build();

    private static List<PropertyDescriptor> DESCRIPTORS;
    private static final Set<Relationship> RELATIONSHIPS;
    static final String TOTAL_ROW_COUNT = "total.row.count";
    static final String ROW_ID_ADDED = "row.id.added";
    static final String FROM_CELL = "from.cell";
    static final String TO_CELL = "to.cell";
    static final String ROW_ID= "row.id";



    static {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(INPUT_DELIMITER);
        descriptors.add(GeoFile);
        descriptors.add(DLon);
        descriptors.add(PLon);
        descriptors.add(DLat);
        descriptors.add(PLat);
        DESCRIPTORS = Collections.unmodifiableList(descriptors);


        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        RELATIONSHIPS = Collections.unmodifiableSet(relationships);
    }


    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }
/*
    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);

        final GeoUtil  geo = new GeoUtil(geofile);

    }
*/




    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);
        final char inputDelimiter = replaceDelimiter(context.getProperty(INPUT_DELIMITER).evaluateAttributeExpressions().getValue());
        final String geofile = context.getProperty(GeoFile).evaluateAttributeExpressions(flowFile).getValue();
        final String dlon = context.getProperty(DLon).evaluateAttributeExpressions(flowFile).getValue();
        final String dlat = context.getProperty(DLat).evaluateAttributeExpressions(flowFile).getValue();
        final String plon = context.getProperty(PLon).evaluateAttributeExpressions(flowFile).getValue();
        final String plat = context.getProperty(PLat).evaluateAttributeExpressions(flowFile).getValue();
        GeoUtil.setFilePath(geofile);




        try {
            AtomicReference<Long> totalRowCount = new AtomicReference<>();
            AtomicReference<String> fromcellvalue = new AtomicReference<>();
            AtomicReference<String> tocellvalue = new AtomicReference<>();
            AtomicReference<String> rowidvalue = new AtomicReference<>();
            AtomicReference<ArrayList<String>> provEvents = new AtomicReference<>();

            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(final InputStream rawIn, final OutputStream rawOut) throws IOException {

                    try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(rawOut));
                         final BufferedReader reader = new BufferedReader(new InputStreamReader(rawIn))) {
                        long rowcount = 0;
                        String fromcell="";
                        String tocell="";
                        String rowId="";
                        String line="";
                        ArrayList<String> rowIds = new ArrayList<>();

                        while((line = reader.readLine()) != null) {
                            rowcount++;
                            rowId = Long.toString(rowcount) +
                                "_" +
                                Long.toString(DateTime.now(DateTimeZone.UTC).getMillis()) +
                                "_" +
                                UUID.randomUUID().toString();

//--------------------------------------------------
                            double pickup_latitude=0;
                            double pickup_longitude=0;
                            double dropoff_latitude=0;
                            double dropoff_longitude=0;

                            try {
                                pickup_longitude = Double.valueOf(plon);
                                pickup_latitude = Double.valueOf(plat);
                            }
                            catch(Exception e2) {
                                e2.printStackTrace();
                            }

                            try {
                                dropoff_longitude = Double.valueOf(dlon);
                                dropoff_latitude = Double.valueOf(dlat);
                            }
                            catch(Exception e1) {
                                e1.printStackTrace();
                            }

                            try {

                                GeoUtil geo = GeoUtil.getInstance();


                                if(geo.contain(pickup_longitude,pickup_latitude)){
                                    fromcell = geo.getCellId();
                                }else{
                                    fromcell = "OutLiner";
                                }

                                if(geo.contain(dropoff_longitude,dropoff_latitude)){
                                    tocell = geo.getCellId();
                                }
                                else{
                                    tocell = "OutLiner";
                                }
//--------------------------------------------------
                            }
                            catch(Exception j) {
                                    j.printStackTrace();
                            }

                            writer.write(rowId + inputDelimiter + line + inputDelimiter + fromcell + inputDelimiter + tocell );
                            writer.newLine();
                            rowIds.add(rowId);
                        }
                        provEvents.set(rowIds);
                        totalRowCount.set(rowcount);
                        fromcellvalue.set(fromcell);
                        tocellvalue.set(tocell);
                        rowidvalue.set(rowId);
                        writer.flush();
                    }
                }
            });
            stopWatch.stop();
            flowFile = session.putAttribute(flowFile, TOTAL_ROW_COUNT,totalRowCount.get().toString());
            flowFile = session.putAttribute(flowFile, FROM_CELL,fromcellvalue.get().toString());
            flowFile = session.putAttribute(flowFile, TO_CELL,tocellvalue.get().toString());
            flowFile = session.putAttribute(flowFile, ROW_ID,rowidvalue.get().toString());
            flowFile = session.putAttribute(flowFile, ROW_ID_ADDED,"true");
            final String tableName = flowFile.getAttribute("tablename");
            for(final String rowId : provEvents.get()){
                final String provEvent = rowId +"," + tableName;
                session.getProvenanceReporter().modifyContent(flowFile, provEvent);
            }
            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getDuration(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);


        }
        catch (final ProcessException e) {
            session.transfer(flowFile, REL_FAILURE);
            throw e;
        }
    }
    private static char replaceDelimiter(String value){
        switch(value.trim()){
            case "\\u0001":
                return 0x01;
            case "\\u0002":
                return 0x02;
            case "\\u0003":
                return 0x03;
            case "\\u0004":
                return 0x04;
            default:
                return value.charAt(0);
        }
    }
}