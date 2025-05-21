/**
 * @file test-google-100.js
 * @description Тестовый скрипт для проверки получения 100 результатов из Google API
 */

require('dotenv').config();
const { XmlRiverApi } = require('./src/xmlriver-api');
const fs = require('fs-extra');
const path = require('path');

async function testGoogle100() {
  console.log('🔍 Тестирование получения 100 результатов из Google API');
  
  // Создаем экземпляр API
  const api = new XmlRiverApi({
    userId: process.env.XMLRIVER_USER_ID,
    apiKey: process.env.XMLRIVER_API_KEY,
    debug: true
  });
  
  // Запрос для тестирования
  const query = 'кредит для бизнеса';
  
  try {
    console.log(`\n🧪 Запрос 100 результатов из Google API в одном запросе`);
    const result = await api.searchGoogle(query, {
      domain: 10,      // google.ru
      country: 2008,   // Россия
      page: 1,         // Первая страница
      groupby: 100,    // 100 результатов
      lr: 'lang_ru'    // Русский язык
    });
    
    console.log(`📋 Параметры запроса: domain=10, country=2008, page=1, groupby=100, lr=lang_ru`);
    
    if (result && result.results) {
      console.log(`✅ Получено ${result.results.length} результатов`);
      
      if (result.results.length > 0) {
        console.log(`\n📊 Первые 5 результатов:`);
        result.results.slice(0, 5).forEach((item, index) => {
          console.log(`   ${index + 1}. ${item.url}`);
        });
        
        // Сохраним результаты в файл для анализа
        const resultsDir = path.join(__dirname, 'data', 'test-results');
        fs.ensureDirSync(resultsDir);
        
        const timestamp = new Date().toISOString().replace(/:/g, '-');
        const resultsFile = path.join(resultsDir, `google-100-results-${timestamp}.json`);
        
        fs.writeJSONSync(resultsFile, {
          query,
          count: result.results.length,
          results: result.results
        }, { spaces: 2 });
        
        console.log(`\n💾 Результаты сохранены в файл: ${resultsFile}`);
      }
    } else {
      console.log(`❌ Нет результатов в ответе: ${JSON.stringify(result)}`);
    }
  } catch (error) {
    console.error(`❌ Ошибка: ${error.message}`);
  }
}

// Запускаем тест
testGoogle100().catch(console.error);
