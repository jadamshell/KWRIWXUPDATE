/**
 * Weather Data Fetcher for GitHub Actions
 * ========================================
 * This script runs hourly to:
 * 1. Fetch the last 2 hours of data from LI-COR sensors
 * 2. Append only new records to Firebase (no duplicates)
 * 3. Keep all historical data
 * 
 * OPTIMIZED: Uses lastTimestamp tracking to avoid downloading
 * all existing data for deduplication. This reduces Firebase
 * downloads by ~95%.
 * 
 * Uses Firebase Admin SDK for secure server-side writes.
 * 
 * Environment variables required (set as GitHub Secrets):
 * - LICOR_API_TOKEN
 * - LICOR_DEVICE_SERIAL
 * - FIREBASE_SERVICE_ACCOUNT (JSON string)
 */

const admin = require('firebase-admin');

// Configuration from environment variables
const LICOR_CONFIG = {
  baseUrl: 'https://api.licor.cloud/v2',
  apiToken: process.env.LICOR_API_TOKEN,
  deviceSerialNumber: process.env.LICOR_DEVICE_SERIAL || '22462095',
};

// Initialize Firebase Admin
let database;
try {
  const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    databaseURL: `https://${serviceAccount.project_id}-default-rtdb.firebaseio.com`
  });
  database = admin.database();
  console.log('✅ Firebase Admin initialized successfully');
} catch (error) {
  console.error('❌ Firebase Admin initialization error:', error.message);
  process.exit(1);
}

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

// Fetch data for a single sensor from LI-COR API
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
        console.log(`    ⏳ Rate limited, waiting ${waitTime}ms...`);
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
        console.error(`    ❌ Failed to fetch: ${error.message}`);
        return { sensorKey, data: [], error: error.message };
      }
      await delay(1000);
    }
  }
}

// Main function
async function main() {
  console.log('🌤️  Weather Data Fetcher (Optimized)');
  console.log('=====================================\n');

  // Validate environment variables
  if (!LICOR_CONFIG.apiToken) {
    console.error('❌ LICOR_API_TOKEN is not set!');
    process.exit(1);
  }

  if (!process.env.FIREBASE_SERVICE_ACCOUNT) {
    console.error('❌ FIREBASE_SERVICE_ACCOUNT is not set!');
    process.exit(1);
  }

  // Calculate time range (last 2 hours to catch hourly updates)
  const endTime = Date.now();
  const startTime = endTime - (2 * 60 * 60 * 1000); // 2 hours ago

  console.log(`📅 Fetching: ${new Date(startTime).toISOString()} → ${new Date(endTime).toISOString()}\n`);

  // Get last timestamps for all sensors (single small read)
  let lastTimestamps = {};
  try {
    const snapshot = await database.ref('sensorMeta').once('value');
    if (snapshot.exists()) {
      lastTimestamps = snapshot.val() || {};
    }
    console.log('📊 Retrieved sensor metadata\n');
  } catch (error) {
    console.log('⚠️ No existing metadata, starting fresh\n');
  }

  // Process all sensors
  const sensorKeys = Object.keys(SENSORS);
  const latestValues = {};
  const newTimestamps = {};
  let totalNewRecords = 0;
  let successfulSensors = 0;

  console.log(`📡 Processing ${sensorKeys.length} sensors...\n`);

  for (let i = 0; i < sensorKeys.length; i++) {
    const sensorKey = sensorKeys[i];
    const sensorConfig = SENSORS[sensorKey];
    const lastTs = lastTimestamps[sensorKey]?.lastTimestamp || 0;

    console.log(`  [${i + 1}/${sensorKeys.length}] ${sensorConfig.name}`);

    // Fetch new data from LI-COR
    const result = await fetchSensor(sensorKey, sensorConfig, startTime, endTime);
    const newRecords = result.data;
    
    if (newRecords.length === 0) {
      console.log(`    📥 0 records from LI-COR`);
      // Keep existing latest value if we have it
      if (lastTimestamps[sensorKey]?.latestValue !== undefined) {
        latestValues[sensorKey] = lastTimestamps[sensorKey].latestValue;
        successfulSensors++;
      }
      await delay(200);
      continue;
    }

    // Filter to only records newer than last timestamp
    const trulyNewRecords = newRecords.filter(r => r.timestamp > lastTs);
    
    console.log(`    📥 ${newRecords.length} from LI-COR, ✨ ${trulyNewRecords.length} new`);

    if (trulyNewRecords.length === 0) {
      // No new records, but update latest value
      latestValues[sensorKey] = newRecords[newRecords.length - 1].value;
      successfulSensors++;
      await delay(200);
      continue;
    }

    // Sort new records by timestamp
    trulyNewRecords.sort((a, b) => a.timestamp - b.timestamp);

    // Get the new latest timestamp and value
    const newestRecord = trulyNewRecords[trulyNewRecords.length - 1];
    latestValues[sensorKey] = newestRecord.value;
    newTimestamps[sensorKey] = {
      lastTimestamp: newestRecord.timestamp,
      latestValue: newestRecord.value,
      lastUpdated: Date.now(),
    };

    // Append new records to Firebase using multi-location update
    // This pushes to the array without reading existing data
    try {
      const updates = {};
      
      // Get current record count
      const countSnapshot = await database.ref(`sensorData/${sensorKey}/recordCount`).once('value');
      let currentCount = countSnapshot.exists() ? countSnapshot.val() : 0;
      
      // Add each new record at the next index
      trulyNewRecords.forEach((record, idx) => {
        updates[`sensorData/${sensorKey}/data/${currentCount + idx}`] = record;
      });
      
      // Update metadata
      updates[`sensorData/${sensorKey}/recordCount`] = currentCount + trulyNewRecords.length;
      updates[`sensorData/${sensorKey}/lastUpdated`] = Date.now();
      updates[`sensorMeta/${sensorKey}`] = newTimestamps[sensorKey];

      await database.ref().update(updates);
      
      console.log(`    💾 Appended ${trulyNewRecords.length} records (total: ${currentCount + trulyNewRecords.length})`);
      
      totalNewRecords += trulyNewRecords.length;
      successfulSensors++;
    } catch (error) {
      console.error(`    ❌ Failed to save: ${error.message}`);
    }

    // Delay between sensors
    await delay(200);
  }

  console.log(`\n📊 Summary:`);
  console.log(`   • Sensors updated: ${successfulSensors}/${sensorKeys.length}`);
  console.log(`   • New records added: ${totalNewRecords}`);

  // Write metadata
  console.log('\n💾 Saving metadata...');
  
  try {
    await database.ref('weatherData/metadata').set({
      timeRange: { endTime },
      fetchedAt: Date.now(),
      cachedAt: Date.now(),
      sensorCount: successfulSensors,
      latestValues: latestValues,
    });
    console.log('   ✅ Metadata saved');
  } catch (error) {
    console.error(`   ❌ Failed: ${error.message}`);
    process.exit(1);
  }

  // Store a history entry (just the latest values, very small)
  try {
    await database.ref(`weatherHistory/${Date.now()}`).set({
      timestamp: Date.now(),
      values: latestValues,
    });
    console.log('   ✅ History entry saved');
  } catch (error) {
    console.warn(`   ⚠️ History save failed: ${error.message}`);
  }

  console.log('\n🎉 Done!\n');
  
  // Exit cleanly
  process.exit(0);
}

// Run
main().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});



