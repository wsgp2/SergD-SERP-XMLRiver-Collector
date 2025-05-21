/**
 * @file serp-collector.js
 * @description Скрипт для сбора данных из поисковой выдачи Яндекса и Google для 100 ключевых фраз
 * @author Sergei Dyshkant (SergD)
 */

require('dotenv').config();
const fs = require('fs-extra');
const path = require('path');
const { XmlRiverApi } = require('./src/xmlriver-api');
const readline = require('readline');

// Код для чтения ключевых фраз из файла
const readKeywordsFromFile = (filePath) => {
  return new Promise((resolve, reject) => {
    try {
      const keywords = [];
      const fileStream = fs.createReadStream(filePath);
      const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
      });

      rl.on('line', (line) => {
        // Пропускаем пустые строки и комментарии
        const trimmedLine = line.trim();
        if (trimmedLine && !trimmedLine.startsWith('#') && !trimmedLine.startsWith('⸻')) {
          keywords.push(trimmedLine);
        }
      });

      rl.on('close', () => {
        console.log(`Сбор данных для ${keywords.length} ключевых фраз и ${Object.keys(MAJOR_CITIES).length} городов`);
        console.log(`Настройки сбора: Yandex - ${CONFIG.collection.yandexPageSize} результатов/страница, Google - ${CONFIG.collection.googlePageSize} результатов/запрос`);
        resolve(keywords);
      });

      rl.on('error', (err) => {
        reject(err);
      });
    } catch (error) {
      reject(error);
    }
  });
};

// Список крупных городов России с населением более 500k человек
const MAJOR_CITIES = {
  'Москва': 213,          // Moscow
  'Санкт-Петербург': 2,   // Saint Petersburg
  'Новосибирск': 65,      // Novosibirsk
  'Екатеринбург': 54,     // Ekaterinburg
  'Казань': 43,           // Kazan
  'Нижний Новгород': 47,  // Nizhny Novgorod
  'Челябинск': 56,        // Chelyabinsk
  'Омск': 66,             // Omsk
  'Самара': 51,           // Samara
  'Ростов-на-Дону': 39,   // Rostov-on-Don
  'Уфа': 172,             // Ufa
  'Красноярск': 62,       // Krasnoyarsk
  'Воронеж': 193,         // Voronezh
  'Пермь': 50,            // Perm
  'Волгоград': 38,        // Volgograd
  'Краснодар': 35,        // Krasnodar
  'Саратов': 194,         // Saratov
  'Тюмень': 197,          // Tyumen
  'Тольятти': 51,         // Tolyatti (используем код Самары)
  'Ижевск': 44,           // Izhevsk
  'Барнаул': 197,         // Barnaul
  'Ульяновск': 195,       // Ulyanovsk
  'Иркутск': 63,          // Irkutsk
  'Хабаровск': 76,        // Khabarovsk
  'Ярославль': 16,        // Yaroslavl
  'Владивосток': 75,      // Vladivostok
  'Махачкала': 28,        // Makhachkala
  'Томск': 67,            // Tomsk
  'Оренбург': 48,         // Orenburg
  'Кемерово': 64,         // Kemerovo
  'Новокузнецк': 64,      // Novokuznetsk (используем код Кемерово)
  'Рязань': 11,           // Ryazan
  'Астрахань': 37,        // Astrakhan
  'Пенза': 49,            // Penza
  'Липецк': 9,            // Lipetsk
  'Киров': 46,            // Kirov
  'Чебоксары': 45,        // Cheboksary
  'Тула': 15,             // Tula
  'Калининград': 22,      // Kaliningrad
  'Брянск': 191,          // Bryansk
  'Иваново': 5,           // Ivanovo
  'Магнитогорск': 56,     // Magnitogorsk (используем код Челябинска)
  'Курск': 8,             // Kursk
  'Тверь': 14,            // Tver
  'Нижний Тагил': 54,     // Nizhny Tagil (используем код Екатеринбурга)
  'Ставрополь': 36,       // Stavropol
  'Улан-Удэ': 69,         // Ulan-Ude
  'Белгород': 4,          // Belgorod
  'Сочи': 239,            // Sochi
  'Севастополь': 959,     // Sevastopol
  'Симферополь': 146,     // Simferopol
};

// Основные настройки скрипта
const CONFIG = {
  // Директория для результатов
  resultsDir: path.join(__dirname, 'data', 'results'),
  
  // Путь к файлу с ключевыми фразами 
  keywordsPath: path.join(__dirname, 'keywords.txt'),
  
  // Настройки сбора
  collection: {
    // Количество уникальных результатов на запрос
    targetUniqueResults: 100,
    
    // Максимальное количество страниц на запрос
    maxPages: 15,
    
    // Размер страницы
    yandexPageSize: 10,
    googlePageSize: 100,
    
    // Задержка между запросами в мс
    delayBetweenRequests: 1000,
    
    // Задержка между городами в мс
    delayBetweenCities: 5000,
    
    // Максимальное количество одновременных потоков
    maxThreads: 5
  },
  
  // Настройки поиска
  search: {
    // Фильтр по времени (2 = месяц)
    within: 2,
    
    // Фильтр похожих результатов
    filter: 1
  },
  
  // Режим отладки
  debug: true
};

/**
 * Сохраняет результаты в файл
 * @param {Object} data - Данные для сохранения
 * @param {string} fileName - Имя файла без расширения
 */
const saveResults = (data, fileName) => {
  try {
    const filePath = path.join(CONFIG.resultsDir, `${fileName}.json`);
    fs.writeJsonSync(filePath, data, { spaces: 2 });
    console.log(`💾 Результаты сохранены: ${filePath}`);
    return filePath;
  } catch (error) {
    console.error(`❌ Ошибка при сохранении результатов: ${error.message}`);
    return null;
  }
};

/**
 * Получает уникальные результаты для заданного запроса и города
 * @param {Object} api - Экземпляр XmlRiverApi
 * @param {string} query - Поисковый запрос
 * @param {Object} city - Информация о городе
 * @param {string} engine - Поисковая система (YANDEX или GOOGLE)
 * @returns {Promise<Array>} - Массив уникальных результатов
 */
const getUniqueResults = async (api, query, city, engine = 'YANDEX') => {
  console.log(`\n🔍 Сбор данных для запроса "${query}" в городе ${city.name} (${engine})`);
  
  // Сет для хранения уникальных URL
  const uniqueUrls = new Set();
  
  // Массив для хранения всех уникальных результатов
  const uniqueResults = [];
  
  // Страница начинается с 0 (для Яндекса)
  let page = 0;
  
  // Константа для преобразования номера страницы в формат 1-based (для логирования)
  const PAGE_OFFSET = 1;
  
  // Пока не собрали нужное количество уникальных результатов и не превысили лимит страниц
  while (uniqueUrls.size < CONFIG.collection.targetUniqueResults && page < CONFIG.collection.maxPages) {
    console.log(`   📄 Обработка страницы ${page + PAGE_OFFSET}/${CONFIG.collection.maxPages}`);
    
    // Общие параметры для обоих поисковых систем
    const baseOptions = {
      page,                        // Используем 0-based пагинацию
      filter: CONFIG.search.filter, // Включаем фильтр похожих результатов
      within: CONFIG.search.within  // Результаты за месяц
    };
    
    let options = { ...baseOptions };
    
    // Настройки для конкретной поисковой системы
    if (engine === 'YANDEX') {
      options.groupby = CONFIG.collection.yandexPageSize;
      options.loc = city.code;
    } else if (engine === 'GOOGLE') {
      options.groupby = CONFIG.collection.googlePageSize; // Получаем 100 результатов за раз
      options.country = 2008; // Код России для Google
      options.domain = 10;    // google.ru вместо google.com
    }
    
    try {
      // Выполняем запрос
      let results;
      if (engine === 'YANDEX') {
        results = await api.searchYandex(query, options);
      } else {
        results = await api.searchGoogle(query, options);
      }
      
      // Проверяем результаты
      if (results && results.results && results.results.length > 0) {
        const pageResults = results.results;
        const initialUniqueCount = uniqueUrls.size;
        
        // Фильтруем только уникальные результаты
        for (const result of pageResults) {
          if (result.url && !uniqueUrls.has(result.url)) {
            uniqueUrls.add(result.url);
            uniqueResults.push({
              ...result,
              page: page + PAGE_OFFSET,
              globalPosition: uniqueResults.length + 1
            });
          }
        }
        
        // Подсчитываем новые уникальные результаты
        const newUniqueCount = uniqueUrls.size - initialUniqueCount;
        
        console.log(`   ✅ Страница ${page + PAGE_OFFSET}: получено ${pageResults.length} результатов (${newUniqueCount} новых уникальных)`);
        
        // Если эта страница не добавила новых уникальных результатов, добавляем счетчик пустых страниц
        if (newUniqueCount === 0) {
          console.log(`   ⚠️ Страница ${page + PAGE_OFFSET} не добавила новых уникальных результатов`);
        }
      } else {
        console.log(`   ⚠️ Страница ${page + PAGE_OFFSET}: нет результатов`);
      }
    } catch (error) {
      console.error(`   ❌ Ошибка при обработке страницы ${page + PAGE_OFFSET}: ${error.message}`);
    }
    
    // Переходим к следующей странице
    page++;
    
    // Пауза между запросами для избежания блокировок
    if (page < CONFIG.collection.maxPages) {
      await new Promise(resolve => setTimeout(resolve, CONFIG.collection.delayBetweenRequests));
    }
  }
  
  // Статистика
  console.log(`\n📊 Собрано ${uniqueResults.length} уникальных результатов за ${page} страниц для запроса "${query}" в городе ${city.name}`);
  
  return uniqueResults;
};

/**
 * Главная функция запуска сбора данных
 */
async function main() {
  try {
    console.log('🚀 Запуск сбора данных для 100 ключевых фраз по крупным городам');
    
    // Создаем директорию для результатов
    fs.ensureDirSync(CONFIG.resultsDir);
    
    // Загружаем ключевые фразы
    const keywords = await readKeywordsFromFile(CONFIG.keywordsPath);
    
    // Формируем список всех городов для сбора данных
    const cities = Object.entries(MAJOR_CITIES).map(([name, code]) => ({
      name,
      code
    }));
    
    console.log(`Всего городов для сбора: ${cities.length}`);
    
    // Инициализируем API клиент
    const api = new XmlRiverApi({
      userId: process.env.XMLRIVER_USER_ID,
      apiKey: process.env.XMLRIVER_API_KEY,
      resultsDir: CONFIG.resultsDir,
      maxThreads: CONFIG.collection.maxThreads,
      debug: CONFIG.debug
    });
    
    // Выводим баланс аккаунта
    try {
      const balance = await api.getBalance();
      console.log(`💰 Текущий баланс: ${balance}`);
    } catch (error) {
      console.error(`❌ Ошибка при получении баланса: ${error.message}`);
    }
    
    // Структура для хранения всех результатов
    const allResults = {
      timestamp: new Date().toISOString(),
      totalKeywords: keywords.length,
      totalCities: cities.length,
      citiesData: {}
    };
    
    // Для каждого города
    for (const city of cities) {
      console.log(`\n🏙️ Обработка города: ${city.name} (код ${city.code})`);
      
      // Инициализируем структуру данных для города
      allResults.citiesData[city.name] = {
        code: city.code,
        keywords: {},
        totalKeywords: keywords.length,
        timestamp: new Date().toISOString()
      };
      
      // Для каждого ключевого слова
      for (const keyword of keywords) {
        // Собираем данные с Яндекса
        const yandexResults = await getUniqueResults(api, keyword, city, 'YANDEX');
        
        // Собираем данные с Google
        const googleResults = await getUniqueResults(api, keyword, city, 'GOOGLE');
        
        // Сохраняем результаты для ключевого слова
        allResults.citiesData[city.name].keywords[keyword] = {
          yandex: {
            totalResults: yandexResults.length,
            uniqueUrls: yandexResults.length,
            results: yandexResults
          },
          google: {
            totalResults: googleResults.length,
            uniqueUrls: googleResults.length,
            results: googleResults
          }
        };
        
        // Сохраняем промежуточные результаты для каждого ключевого слова
        saveResults(
          {
            query: keyword,
            city: city.name,
            cityCode: city.code,
            timestamp: new Date().toISOString(),
            yandex: {
              totalResults: yandexResults.length,
              results: yandexResults
            },
            google: {
              totalResults: googleResults.length,
              results: googleResults
            }
          },
          `${city.name.toLowerCase().replace(/[^a-zа-я0-9]/g, '_')}_${keyword.toLowerCase().replace(/[^a-zа-я0-9]/g, '_')}`
        );
      }
      
      // Сохраняем результаты по городу
      saveResults(
        {
          city: city.name,
          cityCode: city.code,
          timestamp: new Date().toISOString(),
          totalKeywords: keywords.length,
          keywords: allResults.citiesData[city.name].keywords
        },
        `city_${city.name.toLowerCase().replace(/[^a-zа-я0-9]/g, '_')}`
      );
      
      // Пауза между городами
      if (cities.indexOf(city) < cities.length - 1) {
        console.log(`⏳ Пауза ${CONFIG.collection.delayBetweenCities / 1000} секунд перед следующим городом...`);
        await new Promise(resolve => setTimeout(resolve, CONFIG.collection.delayBetweenCities));
      }
    }
    
    // Сохраняем итоговые результаты
    const finalReportPath = saveResults(
      allResults,
      `full_report_${new Date().toISOString().replace(/:/g, '-')}`
    );
    
    console.log(`\n🎉 Сбор данных завершен. Итоговый отчет: ${finalReportPath}`);
    
  } catch (error) {
    console.error(`\n❌ Критическая ошибка: ${error.message}`);
  }
}

// Запускаем процесс, если скрипт вызван напрямую
if (require.main === module) {
  main().catch(console.error);
}

module.exports = { main, getUniqueResults };
