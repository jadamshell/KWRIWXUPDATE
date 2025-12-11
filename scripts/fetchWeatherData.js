/**
 * Weather Data Fetcher for GitHub Actions
 * ========================================
 * This script runs hourly to:
 * 1. Fetch the last 2 hours of data from LI-COR sensors
 * 2. Append new records to existing Firebase data
 * 3. Keep all historical data
 * 
 * Environment variables required (set as GitHub Secrets):
 * - LICOR_API_TOKEN
 * - LICOR_DEVICE_SERIAL
 * - FIREBASE_* credentials
 */

const { initializeApp } = require('firebase/app');
const { getDatabase, ref, set, get } = require('firebase/database');

// Configuration from environment variables
const LICOR_CONFIG = {
  baseUrl: 'https://api.licor.cloud/v2',
  apiToken: process.env.LICOR_API_TOKEN,
  deviceSerialNumber: process.env.LICOR_DEVICE_SERIAL || '22462095',
};

const FIREBASE_CONFIG = {
  apiKey: process.env.FIREBASE_API_KEY,
  authDomain: process.env.FIREBASE_AUTH_DOMAIN,
  databaseURL: process.env.FIREBASE_DATABASE_URL,
  projectId: process.env.FIREBASE_PROJECT_ID,
  storageBucket: process.env.FIREBASE_STORAGE_BUCKET,
  messagingSenderId: process.env.FIREBASE_MESSAGING_SENDER_ID,
  appId: process.env.FIREBASE_APP_ID,
};

// Keep all historical data (no trimming)

// All sensors to fetch (17 total)
const SENSORS = {
  barometricPressure: { sn: '21956394-1', name: 'Barometric Pressure', unit: 'mbar' },
  precipitation: { sn: '21987752-1', name: 'Precipitation', unit: 'mm' },
  rainfall24hr: { sn: '21987752-2', name: 'Rainfall (24-Hr)', unit: 'mm' },
  rainfallWeekly: { sn: '21987752-3', name: 'Rainfall (Weekly)', unit: 'mm' },
  rainfallMonthly: { sn: '21987752-4', name: 'Rainfall (Monthly)', unit: 'mm' },
  waterLevel: { sn: '22054593-1', name: 'Water Level', unit: 'm' },
  diffPressure: { sn: '22054593-2', name: 'Diff Pressure', unit: 'kPa' },
  waterTemperature: { sn: '22054593-3', name: 'Water Temperature', unit: '°C' },
  waterBaroPressure: { sn: '22054593-4', name: 'Baro Pressure (Water)', unit: 'kPa' },
  solarRadiation: { sn: '22430939-1', name: 'Solar Radiation', unit: 'W/m²' },
  temperature: { sn: '22442709-1', name: 'Air Temperature', unit: '°C' },
  humidity: { sn: '22442709-2', name: 'Relative Humidity', unit: '%' },
  dewPoint: { sn: '22442709-3', name: 'Dew Point', unit: '°C' },
  windSpeed: { sn: '22447153-1', name: 'Wind Speed', unit: 'm/s' },
  gustSpeed: { sn: '22447153-2', name: 'Gust Speed', unit: 'm/s' },
  windDirection: { sn: '22447153-3', name: 'Wind Direction', unit: '°' },
  evapotranspiration: { sn: '22462095-1', name: 'Reference ET', unit: 'mm' },
};

// Delay helper
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Fetch data for a single sensor
async function fetchSensor(sensorKey, sensorConfig, startTime, endTime, retries = 3) {
  const url = new URL(`${LICOR_CONFIG.baseUrl}/data`);
  url.searchParams.append('deviceSerialNumber', LICOR_CONFIG.deviceSerialNumber);
  url.searchParams.append('sensorSerialNumber', sensorConfig.sn);
  url.searchParams.append('startTime', startTime.toString());
  url.searchParams.append('endTime', endTime.toString());

  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const response = await fetch(url.toString(), {
        method: 'GET',
        headers: {
          'accept': 'application/json',
          'Authorization': `Bearer ${LICOR_CONFIG.apiToken}`,
        },
      });

      if (response.status === 429) {
        const waitTime = Math.pow(2, attempt) * 2000;
        console.log(`  ⏳ Rate limited on ${sensorKey}, waiting ${waitTime}ms...`);
        await delay(waitTime);
        continue;
      }

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const data = await response.json();
      const records = data?.sensors?.[0]?.data?.[0]?.records || [];
      
      return {
        sensorKey,
        data: records.map(([timestamp, value]) => ({
          timestamp,
          value,
          sensorKey,
          unit: sensorConfig.unit,
        })),
      };
    } catch (error) {
      if (attempt === retries - 1) {
        console.error(`  ❌ Failed to fetch ${sensorKey}: ${error.message}`);
        return { sensorKey, data: [], error: error.message };
      }
      await delay(1000);
    }
  }
}

// Main function
async function main() {
  console.log('🌤️  Weather Data Fetcher (Hourly Append Mode)');
  console.log('=============================================\n');

  // Validate environment variables
  if (!LICOR_CONFIG.apiToken) {
    console.error('❌ LICOR_API_TOKEN is not set!');
    process.exit(1);
  }

  if (!FIREBASE_CONFIG.databaseURL) {
    console.error('❌ FIREBASE_DATABASE_URL is not set!');
    process.exit(1);
  }

  // Initialize Firebase
  console.log('🔥 Initializing Firebase...');
  const app = initializeApp(FIREBASE_CONFIG);
  const database = getDatabase(app);

  // Calculate time range (last 2 hours to catch hourly updates)
  const endTime = Date.now();
  const startTime = endTime - (2 * 60 * 60 * 1000); // 2 hours ago

  console.log(`📅 Fetching new data from ${new Date(startTime).toISOString()} to ${new Date(endTime).toISOString()}\n`);

  // Fetch all sensors sequentially
  const sensorKeys = Object.keys(SENSORS);
  const latestValues = {};
  let totalNewRecords = 0;
  let successfulSensors = 0;

  console.log(`📡 Fetching ${sensorKeys.length} sensors from LI-COR API...\n`);

  for (let i = 0; i < sensorKeys.length; i++) {
    const sensorKey = sensorKeys[i];
    const sensorConfig = SENSORS[sensorKey];

    console.log(`  [${i + 1}/${sensorKeys.length}] ${sensorConfig.name}...`);

    // Fetch new data from LI-COR
    const result = await fetchSensor(sensorKey, sensorConfig, startTime, endTime);
    const newRecords = result.data;
    
    console.log(`    📥 ${newRecords.length} new records from LI-COR`);

    // Get existing data from Firebase
    let existingData = [];
    try {
      const snapshot = await get(ref(database, `sensorData/${sensorKey}/data`));
      if (snapshot.exists()) {
        existingData = snapshot.val() || [];
      }
    } catch (error) {
      console.log(`    ⚠️ No existing data found, starting fresh`);
    }

    console.log(`    📦 ${existingData.length} existing records in Firebase`);

    // Create a Set of existing timestamps for fast lookup
    const existingTimestamps = new Set(existingData.map(r => r.timestamp));

    // Filter new records to only include ones we don't already have
    const trulyNewRecords = newRecords.filter(r => !existingTimestamps.has(r.timestamp));

    console.log(`    ✨ ${trulyNewRecords.length} truly new records to add`);

    // Merge: existing + new
    const mergedData = [...existingData, ...trulyNewRecords];

    // Sort by timestamp
    mergedData.sort((a, b) => a.timestamp - b.timestamp);

    // Update latest value
    if (mergedData.length > 0) {
      successfulSensors++;
      latestValues[sensorKey] = mergedData[mergedData.length - 1].value;
      totalNewRecords += trulyNewRecords.length;
    }

    // Save to Firebase
    if (mergedData.length > 0) {
      try {
        await set(ref(database, `sensorData/${sensorKey}`), {
          data: mergedData,
          lastUpdated: Date.now(),
          recordCount: mergedData.length,
        });
        console.log(`    💾 Saved ${mergedData.length} total records`);
      } catch (error) {
        console.error(`    ❌ Failed to save ${sensorKey}: ${error.message}`);
      }
    }

    // Delay between requests
    if (i < sensorKeys.length - 1) {
      await delay(200);
    }
  }

  console.log(`\n📊 Summary:`);
  console.log(`   - Sensors updated: ${successfulSensors}/${sensorKeys.length}`);
  console.log(`   - New records added: ${totalNewRecords}`);

  // Write metadata
  console.log('\n💾 Writing metadata to Firebase...');
  
  try {
    await set(ref(database, 'weatherData/metadata'), {
      timeRange: { endTime },
      fetchedAt: Date.now(),
      cachedAt: Date.now(),
      sensorCount: successfulSensors,
      latestValues: latestValues,
    });
    console.log('   ✅ Metadata saved successfully!');
  } catch (error) {
    console.error(`   ❌ Failed to write metadata: ${error.message}`);
    process.exit(1);
  }

  // Store a history entry
  const historyRef = ref(database, `weatherHistory/${Date.now()}`);
  try {
    await set(historyRef, {
      timestamp: Date.now(),
      values: latestValues,
    });
    console.log('   ✅ History entry saved!\n');
  } catch (error) {
    console.warn(`   ⚠️ Failed to save history: ${error.message}`);
  }

  console.log('🎉 Done!\n');
  
  // Exit cleanly (Firebase keeps connection open otherwise)
  process.exit(0);
}

// Run
main().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});

