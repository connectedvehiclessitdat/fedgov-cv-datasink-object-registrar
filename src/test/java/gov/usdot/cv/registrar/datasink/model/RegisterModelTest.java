package gov.usdot.cv.registrar.datasink.model;

import gov.usdot.cv.registrar.datasink.RegisterModel;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

public class RegisterModelTest {

	@Test
	public void testModelDeserialization() throws IOException {
		String jsonFile = "src/test/resources/register_good.json";
		String json = FileUtils.readFileToString(new File(jsonFile));
		RegisterModel model = RegisterModel.fromJSON(json);
		model.validate();
	}
}
