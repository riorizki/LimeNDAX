const { exec } = require('child_process');
const path = require('path');
const fs = require('fs').promises;

/**
 * Run the Python checker script with a JSON file path
 * @param {string} jsonPath - Path to the JSON file to analyze
 * @returns {Promise<Object>} - Promise resolving to the JSON response
 */
async function runCheckerFromFile(jsonPath) {
  return new Promise((resolve, reject) => {
    const pythonScript = path.join(__dirname, 'checek.py');
    const command = `python3 "${pythonScript}" "${jsonPath}"`;
    
    exec(command, (error, stdout, stderr) => {
      if (error) {
        reject(new Error(`Python script error: ${error.message}`));
        return;
      }
      
      if (stderr) {
        console.warn('Python stderr:', stderr);
      }
      
      try {
        const result = JSON.parse(stdout);
        resolve(result);
      } catch (parseError) {
        reject(new Error(`Failed to parse JSON response: ${parseError.message}`));
      }
    });
  });
}

/**
 * Run the Python checker script with JSON data directly
 * @param {Object} jsonData - JSON data to analyze
 * @returns {Promise<Object>} - Promise resolving to the JSON response
 */
async function runCheckerFromJSON(jsonData) {
  const tempFilePath = path.join(__dirname, 'temp_input.json');
  
  try {
    // Write JSON data to temporary file
    await fs.writeFile(tempFilePath, JSON.stringify(jsonData, null, 2));
    
    // Run checker with temp file
    const result = await runCheckerFromFile(tempFilePath);
    
    // Clean up temp file
    await fs.unlink(tempFilePath);
    
    return result;
  } catch (error) {
    // Clean up temp file on error
    try {
      await fs.unlink(tempFilePath);
    } catch (unlinkError) {
      // Ignore unlink error
    }
    throw error;
  }
}

/**
 * Run the Python checker script with JSON string
 * @param {string} jsonString - JSON string to analyze
 * @returns {Promise<Object>} - Promise resolving to the JSON response
 */
async function runCheckerFromString(jsonString) {
  try {
    const jsonData = JSON.parse(jsonString);
    return await runCheckerFromJSON(jsonData);
  } catch (parseError) {
    throw new Error(`Invalid JSON string: ${parseError.message}`);
  }
}

/**
 * Run the Python checker script with default test file
 * @returns {Promise<Object>} - Promise resolving to the JSON response
 */
async function runCheckerDefault() {
  const defaultPath = "/Users/riorizki/development/repos/ION-Mobility-Team/mes/LimeNDAX/result/LG_2_EOL_test_15-1-4-20250428125222_response.json";
  return await runCheckerFromFile(defaultPath);
}

/**
 * Batch process multiple JSON files
 * @param {string[]} filePaths - Array of JSON file paths to analyze
 * @returns {Promise<Object[]>} - Array of results for each file
 */
async function batchProcessFiles(filePaths) {
  const results = [];
  
  for (const filePath of filePaths) {
    try {
      const result = await runCheckerFromFile(filePath);
      results.push({ filePath, result, success: true });
    } catch (error) {
      results.push({ filePath, error: error.message, success: false });
    }
  }
  
  return results;
}

// Export functions for use in other modules
module.exports = {
  runCheckerFromFile,
  runCheckerFromJSON,
  runCheckerFromString,
  runCheckerDefault,
  batchProcessFiles
};

// Example usage (uncomment to test)
/*
async function test() {
  try {
    // Test with default file
    const result = await runCheckerDefault();
    console.log('Step 1 results:', result.tests[0].results);
    
    // Test with JSON data
    const testData = {
      "data": {
        "step": [
          {
            "step_type": "Rest",
            "step_time": "00:00:10",
            "oneset_date": "2024-01-01 10:00:00",
            "end_date": "2024-01-01 10:00:10"
          }
        ],
        "auxDBC": []
      }
    };
    
    const testResult = await runCheckerFromJSON(testData);
    console.log('Test result:', testResult);
    
  } catch (error) {
    console.error('Error:', error.message);
  }
}

if (require.main === module) {
  test();
}
*/