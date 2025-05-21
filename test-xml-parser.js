/**
 * @file test-xml-parser.js
 * @description Тестирование парсинга XML ответа от XML River API
 */

require('dotenv').config();
const fs = require('fs-extra');
const path = require('path');
const axios = require('axios');
const xml2js = require('xml2js');
const { promisify } = require('util');
const parseXml = promisify(xml2js.parseString);

// Базовые URL для API
const API_BASE_URL = {
  GOOGLE: 'http://xmlriver.com/search/xml',
  YANDEX: 'http://xmlriver.com/search_yandex/xml'
};

/**
 * Выполняет запрос к XML River API и выводит подробную информацию о структуре ответа
 * @param {string} query - Поисковый запрос
 * @param {string} engine - Поисковая система (GOOGLE или YANDEX)
 * @param {Object} options - Параметры запроса
 */
async function testSearch(query, engine, options = {}) {
  try {
    console.log(`🔍 Тестирование ${engine} запроса: "${query}"`);
    console.log(`📋 Параметры: ${JSON.stringify(options)}`);
    
    const baseUrl = engine === 'GOOGLE' ? API_BASE_URL.GOOGLE : API_BASE_URL.YANDEX;
    const safeQuery = encodeURIComponent(query);
    
    // Базовые параметры для запроса
    const params = {
      user: process.env.XMLRIVER_USER_ID,
      key: process.env.XMLRIVER_API_KEY,
      query: safeQuery,
      ...options
    };
    
    // Формируем URL запроса
    const queryString = Object.entries(params)
      .map(([key, value]) => `${key}=${encodeURIComponent(value)}`)
      .join('&');
    
    const requestUrl = `${baseUrl}?${queryString}`;
    console.log(`🌐 URL запроса: ${requestUrl}`);
    
    // Выполняем запрос
    const response = await axios.get(requestUrl, { timeout: 60000 });
    
    // Проверяем ответ
    if (response.status !== 200) {
      throw new Error(`Ошибка HTTP ${response.status}: ${response.statusText}`);
    }
    
    // Сохраняем сырой ответ для отладки
    const rawFilePath = path.join(__dirname, 'data', 'debug', `raw_${engine.toLowerCase()}_${new Date().toISOString().replace(/:/g, '-')}.xml`);
    fs.ensureDirSync(path.dirname(rawFilePath));
    fs.writeFileSync(rawFilePath, response.data);
    
    // Проверяем наличие ошибки в ответе
    if (typeof response.data === 'string' && response.data.includes('<error')) {
      const errorMatch = response.data.match(/<error code="([^"]+)">([^<]+)<\/error>/);
      if (errorMatch) {
        throw new Error(`Ошибка API ${errorMatch[1]}: ${errorMatch[2]}`);
      }
    }
    
    // Парсим XML ответ
    const parsedXml = await parseXml(response.data);
    
    // Анализируем структуру ответа
    console.log('\n📊 Структура XML ответа:');
    if (!parsedXml || !parsedXml.yandexsearch || !parsedXml.yandexsearch.response) {
      console.log('❌ Не удалось найти основные блоки в ответе API');
      return;
    }
    
    const xmlResponse = parsedXml.yandexsearch.response[0];
    
    // Проверяем наличие ошибки
    if (xmlResponse.error) {
      const errorCode = xmlResponse.error[0].$.code;
      const errorText = xmlResponse.error[0]._;
      console.log(`❌ Ошибка API ${errorCode}: ${errorText}`);
      return;
    }
    
    // Выводим информацию о найденных результатах
    if (xmlResponse.found && xmlResponse.found.length > 0) {
      console.log(`📈 Найдено документов: ${xmlResponse.found[0]._} (priority: ${xmlResponse.found[0].$.priority})`);
    }
    
    // Проверяем блок результатов
    if (!xmlResponse.results || !xmlResponse.results[0] || !xmlResponse.results[0].grouping) {
      console.log('❌ Блок results.grouping не найден в ответе');
      return; 
    }
    
    const grouping = xmlResponse.results[0].grouping[0];
    
    // Информация о странице
    if (grouping.page && grouping.page.length > 0) {
      const page = grouping.page[0];
      console.log(`📄 Страница: ${page._} (Позиции: ${page.$.first} - ${page.$.last})`);
    }
    
    // Проверяем наличие групп документов
    if (!grouping.group || grouping.group.length === 0) {
      console.log('❌ Группы документов не найдены в ответе');
      console.log('📋 Полный ответ API:', JSON.stringify(xmlResponse, null, 2));
      return;
    }
    
    // Подсчитываем количество результатов
    const resultsCount = grouping.group.length;
    console.log(`🔢 Количество групп в ответе: ${resultsCount}`);
    
    // Выводим информацию о первых двух документах для примера
    for (let i = 0; i < Math.min(2, resultsCount); i++) {
      const group = grouping.group[i];
      if (group && group.doc && group.doc.length > 0) {
        const doc = group.doc[0];
        
        console.log(`\n📑 Документ #${i + 1}:`);
        if (doc.url && doc.url.length > 0) console.log(`   🔗 URL: ${doc.url[0]}`);
        if (doc.title && doc.title.length > 0) console.log(`   📌 Заголовок: ${doc.title[0]}`);
        
        if (doc.passages && doc.passages.length > 0 && 
            doc.passages[0].passage && doc.passages[0].passage.length > 0) {
          console.log(`   📝 Сниппет: ${doc.passages[0].passage[0]}`);
        }
      } else {
        console.log(`❌ Документ #${i + 1} не содержит необходимых данных`);
      }
    }
    
    // Выводим информацию о всех результатах
    const allResults = extractResults(parsedXml, engine);
    console.log(`\n📊 Извлечено результатов: ${allResults.length}`);
    
    // Выводим первые 2 результата для примера
    if (allResults.length > 0) {
      for (let i = 0; i < Math.min(2, allResults.length); i++) {
        console.log(`\n🔍 Результат #${i + 1}:`);
        console.log(`   URL: ${allResults[i].url}`);
        console.log(`   Заголовок: ${allResults[i].title}`);
      }
    }
    
  } catch (error) {
    console.error(`❌ Ошибка при выполнении запроса: ${error.message}`);
    if (error.stack) console.error(error.stack);
  }
}

/**
 * Извлекает результаты из XML ответа
 * @param {Object} parsedXml - Распарсенный XML ответ
 * @param {string} engine - Поисковая система (GOOGLE или YANDEX)
 * @returns {Array} - Массив результатов поиска
 */
function extractResults(parsedXml, engine) {
  const results = [];
  
  try {
    if (!parsedXml || !parsedXml.yandexsearch || !parsedXml.yandexsearch.response) {
      console.error('❌ Не удалось найти основные блоки в ответе API');
      return results;
    }
    
    const xmlResponse = parsedXml.yandexsearch.response[0];
    
    // Проверяем наличие ошибки
    if (xmlResponse.error) {
      console.error(`❌ Ошибка API: ${xmlResponse.error[0]._}`);
      return results;
    }
    
    // Проверяем блок результатов
    if (!xmlResponse.results || !xmlResponse.results[0] || !xmlResponse.results[0].grouping) {
      console.error('❌ Блок results.grouping не найден в ответе');
      return results;
    }
    
    const grouping = xmlResponse.results[0].grouping[0];
    
    // Проверяем наличие групп документов
    if (!grouping.group || grouping.group.length === 0) {
      console.error('❌ Группы документов не найдены в ответе');
      return results;
    }
    
    // Извлекаем информацию о каждом документе
    grouping.group.forEach((group, index) => {
      if (group && group.doc && group.doc.length > 0) {
        const doc = group.doc[0];
        
        let url = '';
        let title = '';
        let snippet = '';
        
        if (doc.url && doc.url.length > 0) url = doc.url[0];
        if (doc.title && doc.title.length > 0) title = doc.title[0];
        
        if (doc.passages && doc.passages.length > 0 && 
            doc.passages[0].passage && doc.passages[0].passage.length > 0) {
          snippet = doc.passages[0].passage[0];
        }
        
        if (url) {
          results.push({
            url,
            title,
            snippet,
            position: index + 1,
            engine
          });
        }
      }
    });
    
  } catch (error) {
    console.error(`❌ Ошибка при извлечении результатов: ${error.message}`);
  }
  
  return results;
}

// Тесты для Яндекса и Google
async function runTests() {
  // Запрос для тестирования
  const query = 'кредит для бизнеса';
  
  // Тест Яндекса
  await testSearch(query, 'YANDEX', {
    groupby: 10,
    page: 0,  // Нумерация страниц в Яндексе с 0
    filter: 1,
    loc: 213, // Москва
    lr: 'lang_ru'
  });
  
  console.log('\n' + '='.repeat(80) + '\n');
  
  // Тест Google
  await testSearch(query, 'GOOGLE', {
    groupby: 10,
    page: 1,  // Нумерация страниц в Google с 1
    filter: 1,
    country: 2008, // Россия
    domain: 10,    // google.ru
    lr: 'lang_ru'
  });
}

// Запуск тестов
runTests().catch(console.error);
