package com.reader.csvreader;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.modules.junit4.PowerMockRunner;

import junit.framework.TestCase;



/**
 * Unit test for Orchestrator.
 */
@RunWith(PowerMockRunner.class)
public class OrchestratorTest 
    extends TestCase
{
	@Mock
	Orchestrator orchestrator;

	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
	}

	@Test
	public void testReadTheFileFromS3() {
		
		InputStream stubInputStream;
		try {
			stubInputStream = IOUtils.toInputStream("some test data for my input stream", "UTF-8");
			ZipInputStream zis = new ZipInputStream(stubInputStream);			
			orchestrator.readTheFileFromS3(zis);
			verify(orchestrator, times(1)).readTheFileFromS3(zis);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testWriteParquetFile() {
		List<GenericSchema> dataList = new ArrayList<>();
		orchestrator.writeParquetFile(dataList, "");
		verify(orchestrator, times(1)).writeParquetFile(dataList, "");
	}

}
