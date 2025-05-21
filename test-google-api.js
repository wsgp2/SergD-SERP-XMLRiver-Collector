/**
 * @file test-google-api.js
 * @description Тестовый скрипт для проверки работы Google API в XMLRiver
 */

require('dotenv').config();
const { XmlRiverApi } = require('./src/xmlriver-api');

async function testGoogleAPI() {
  console.log('🔍 Тестирование Google API XMLRiver');
  
  // Создаем экземпляр API
  const api = new XmlRiverApi({
    userId: process.env.XMLRIVER_USER_ID,
    apiKey: process.env.XMLRIVER_API_KEY,
    debug: true
  });
  
  // Запросы для тестирования
  const query = 'кредит для бизнеса';
  
  console.log(`\n🧪 Тест 1: Базовый запрос к Google API`);
  try {
    const result1 = await api.searchGoogle(query, {
      domain: 10,    // google.ru
      country: 2008, // Россия
      page: 1,       // Для Google первая страница = 1 (не 0)
      groupby: 10,   // 10 результатов
      lr: 'lang_ru'  // Русский язык
    });
    
    console.log(`📋 Параметры запроса: domain=10, country=2008, page=1, groupby=10, lr=lang_ru`);
    
    if (result1 && result1.results && result1.results.length > 0) {
      console.log(`✅ Получено ${result1.results.length} результатов`);
      console.log(`   Первый URL: ${result1.results[0].url}`);
    } else {
      console.log(`❌ Нет результатов в ответе: ${JSON.stringify(result1)}`);
    }
  } catch (error) {
    console.error(`❌ Ошибка: ${error.message}`);
  }
  
  console.log(`\n🧪 Тест 2: Google API с пагинацией через page=2`);
  try {
    const result2 = await api.searchGoogle(query, {
      domain: 10,    // google.ru
      country: 2008, // Россия
      groupby: 10,   // 10 результатов
      page: 2,       // Вторая страница для Google
      lr: 'lang_ru'  // Русский язык
    });
    
    console.log(`📋 Параметры запроса: domain=10, country=2008, page=2, groupby=10, lr=lang_ru`);
    
    if (result2 && result2.results && result2.results.length > 0) {
      console.log(`✅ Получено ${result2.results.length} результатов`);
      console.log(`   Первый URL: ${result2.results[0].url}`);
    } else {
      console.log(`❌ Нет результатов в ответе: ${JSON.stringify(result2)}`);
    }
  } catch (error) {
    console.error(`❌ Ошибка: ${error.message}`);
  }
  
  console.log(`\n🧪 Тест 3: Google API с параметром filter и временным периодом`);
  try {
    const result3 = await api.searchGoogle(query, {
      domain: 10,     // google.ru
      country: 2008,  // Россия
      page: 1,        // Первая страница
      groupby: 10,    // 10 результатов
      lr: 'lang_ru',  // Русский язык
      filter: 1,      // Фильтрация дубликатов
      tbs: 'qdr:m'    // Результаты за месяц
    });
    
    console.log(`📋 Параметры запроса: domain=10, country=2008, page=1, groupby=10, lr=lang_ru, filter=1, tbs=qdr:m`);
    
    if (result3 && result3.results && result3.results.length > 0) {
      console.log(`✅ Получено ${result3.results.length} результатов`);
      console.log(`   Первый URL: ${result3.results[0].url}`);
    } else {
      console.log(`❌ Нет результатов в ответе: ${JSON.stringify(result3)}`);
    }
  } catch (error) {
    console.error(`❌ Ошибка: ${error.message}`);
  }
  
  console.log(`\n🧪 Тест 4: Прямой вызов performSearch для Google с device=desktop`);
  try {
    // Прямой вызов базового метода для исключения ошибок в обертке
    const result4 = await api.performSearch(query, 'GOOGLE', {
      domain: 10,     // google.ru
      country: 2008,  // Россия
      page: 1,        // Первая страница
      groupby: 10,    // 10 результатов
      lr: 'lang_ru',  // Русский язык
      device: 'desktop' // Тип устройства
    });
    
    console.log(`📋 Параметры запроса: domain=10, country=2008, page=1, groupby=10, lr=lang_ru, device=desktop`);
    
    if (result4 && result4.results && result4.results.length > 0) {
      console.log(`✅ Получено ${result4.results.length} результатов`);
      console.log(`   Первый URL: ${result4.results[0].url}`);
    } else {
      console.log(`❌ Нет результатов в ответе: ${JSON.stringify(result4)}`);
    }
  } catch (error) {
    console.error(`❌ Ошибка: ${error.message}`);
  }
}

// Добавляем еще пару тестов

console.log(`\n🧪 Тест 5: Google API с минимальными параметрами`);
try {
  // Вызываем в отдельной функции для наглядности
  const test5 = async () => {
    const result5 = await api.searchGoogle(query, {
      page: 1,        // Только страница
      groupby: 10     // И количество результатов
    });
    
    console.log(`📋 Параметры запроса: page=1, groupby=10`);
    
    if (result5 && result5.results && result5.results.length > 0) {
      console.log(`✅ Получено ${result5.results.length} результатов`);
      console.log(`   Первый URL: ${result5.results[0].url}`);
    } else {
      console.log(`❌ Нет результатов в ответе: ${JSON.stringify(result5)}`);
    }
  };
  
  // Запускаем тесты
  testGoogleAPI()
    .then(() => test5())
    .catch(console.error);
} catch (error) {
  console.error(`❌ Ошибка: ${error.message}`);
}
