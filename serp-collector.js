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
        // Пропускаем пустые строки, комментарии и заголовки категорий (заканчиваются на ':')
        const trimmedLine = line.trim();
        if (trimmedLine && 
            !trimmedLine.startsWith('#') && 
            !trimmedLine.startsWith('⸼') && 
            !trimmedLine.endsWith(':')) {
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
  
  // Директория для промежуточных результатов
  intermediateDir: path.join(__dirname, 'data', 'intermediate'),
  
  // Путь к файлу с ключевыми фразами 
  keywordsFile: path.join(__dirname, 'keywords.txt'),
  
  // Параметры подключения
  connection: {
    timeout: 60000,  // Таймаут для запросов (мс)
    retries: 3       // Количество попыток при ошибке
  },
  
  // Параметры сбора данных
  collection: {
    // Результатов на страницу для Яндекса (макс 100)
    yandexPageSize: 10,
    
    // Результатов на запрос для Google (макс 100)
    googlePageSize: 100,
    
    // Целевое количество уникальных результатов для запроса
    targetUniqueResults: 100,
    
    // Максимальное количество страниц для обработки
    maxPages: 5,
    
    // Задержка между запросами (мс)
    delayBetweenRequests: 500,  // Увеличено на 20% с 500 до 500
    
    // Задержка между городами (мс)
    delayBetweenCities: 2000,   // Увеличено на 20% с 2000 до 2000
    
    // Параметры автоматических повторных попыток
    maxRetries: 5,             // Максимальное количество повторных попыток (увеличено с 3 до 5)
    initialBackoff: 1000,      // Начальное время ожидания (мс) - увеличено на 20%
    
    // Автоматическое сохранение промежуточных результатов
    saveIntermediate: true,    // Включить/выключить
    saveInterval: 10           // Интервал сохранения (количество ключевых фраз)
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

// Создаем директории для сохранения результатов
fs.ensureDirSync(CONFIG.resultsDir);
fs.ensureDirSync(CONFIG.intermediateDir);

/**
 * Сохраняет результаты в файл
 * @param {Object} data - Данные для сохранения
 * @param {string} fileName - Имя файла без расширения
 * @param {boolean} isIntermediate - Флаг для сохранения в директорию промежуточных результатов
 * @returns {string|null} - Путь к сохраненному файлу или null в случае ошибки
 */
const saveResults = (data, fileName, isIntermediate = false) => {
  const timestamp = new Date().toISOString().replace(/:/g, '-');
  const targetDir = isIntermediate ? CONFIG.intermediateDir : CONFIG.resultsDir;
  const filePath = path.join(targetDir, `${fileName}.json`);
  
  try {
    fs.writeJsonSync(filePath, data, { spaces: 2 });
    if (!isIntermediate || CONFIG.debug) {
      console.log(`💾 ${isIntermediate ? 'Промежуточные' : 'Окончательные'} результаты сохранены в ${filePath}`);
    }
    return filePath;
  } catch (error) {
    console.error(`❌ Ошибка при сохранении результатов: ${error.message}`);
    return null;
  }
};

/**
 * Загружает промежуточные результаты из файла
 * @param {string} fileName - Имя файла без расширения
 * @returns {Object|null} - Загруженные данные или null в случае ошибки
 */
const loadIntermediateResults = (fileName) => {
  const filePath = path.join(CONFIG.intermediateDir, `${fileName}.json`);
  
  try {
    if (fs.existsSync(filePath)) {
      const data = fs.readJsonSync(filePath);
      console.log(`📂 Загружены промежуточные результаты из ${filePath}`);
      return data;
    }
    return null;
  } catch (error) {
    console.error(`⚠️ Не удалось загрузить промежуточные результаты: ${error.message}`);
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
  
  // Время начала сбора данных для этого запроса
  const startTime = Date.now();
  
  // Страница начинается с 0 (для Яндекса) или 1 (для Google)
  let page = engine === 'YANDEX' ? 0 : 1;
  
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
      // Выполняем запрос с поддержкой автоматических повторных попыток
      // для определенных типов ошибок
      let results;
      let retries = 0;
      const maxRetries = CONFIG.collection.maxRetries || 5;  // Увеличиваем количество повторных попыток с 3 до 5
      const initialBackoff = CONFIG.collection.initialBackoff || 2400; // Увеличено на 20% с 2000 до 2400 мс
      let backoff = initialBackoff;
      
      while (retries <= maxRetries) {
        try {
          if (engine === 'YANDEX') {
            results = await api.searchYandex(query, options);
          } else {
            results = await api.searchGoogle(query, options);
          }
          break; // Успешно получили результаты, выходим из цикла
        } catch (requestError) {
          // Проверяем, можем ли мы повторить запрос для этой ошибки
          const errorCode = requestError.code || 0;
          // Все ошибки 500, 111 и ошибки с сообщением о выполнении перезапроса считаем могущими быть повторенными
          const isRetryableError = [500, 111].includes(errorCode) || 
                                 requestError.message.includes('Выполните перезапрос') ||  
                                 requestError.message.includes('Ответ от поисковой системы не получен') ||
                                 requestError.message.includes('Нет свободных каналов');
          
          if (isRetryableError && retries < maxRetries) {
            retries++;
            console.log(`   ⚠️ Ошибка API (код ${errorCode}): ${requestError.message}. Повторная попытка ${retries}/${maxRetries} через ${backoff/1000} сек...`);
            
            // Экспоненциальный откат
            await new Promise(resolve => setTimeout(resolve, backoff));
            backoff *= 2; // Увеличиваем время ожидания экспоненциально
          } else {
            // Не можем повторить или исчерпали попытки
            throw requestError;
          }
        }
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
        
        // Рассчитываем прогресс и оставшееся время
        const currentProgress = Math.min(uniqueUrls.size / CONFIG.collection.targetUniqueResults, 1);
        const elapsedTime = (Date.now() - startTime) / 1000; // в секундах
        let remainingTime = 0;
        
        if (currentProgress > 0 && currentProgress < 1) {
          // Оценка оставшегося времени на основе текущей скорости
          remainingTime = (elapsedTime / currentProgress) * (1 - currentProgress);
        }
        
        // Форматируем оставшееся время
        const remainingMinutes = Math.floor(remainingTime / 60);
        const remainingSeconds = Math.floor(remainingTime % 60);
        const timeStr = remainingMinutes > 0 ? 
          `${remainingMinutes}м ${remainingSeconds}с` : 
          `${remainingSeconds}с`;

        // Рассчитываем среднюю скорость сбора (результатов в минуту)
        const collectionRate = uniqueUrls.size / (elapsedTime / 60 || 0.1);
        const rateStr = collectionRate.toFixed(1);
        
        // Рассчитываем процент дублирующихся результатов
        const duplicateRate = pageResults.length > 0 ? 
          ((pageResults.length - newUniqueCount) / pageResults.length * 100).toFixed(1) : 
          '0.0';
        
        console.log(`   ✅ Страница ${page + PAGE_OFFSET}: получено ${pageResults.length} результатов (${newUniqueCount} новых уникальных)`);
        console.log(`   📊 Прогресс: ${Math.floor(currentProgress * 100)}% | ${uniqueUrls.size}/${CONFIG.collection.targetUniqueResults} URL | Дубликаты: ${duplicateRate}%`);
        console.log(`   ⏱️ Скорость: ${rateStr} рез/мин | Осталось: ~${timeStr}`);
        
        // Создаем сигнатуру прогресса для визуализации
        const progressBarWidth = 20;
        const filledWidth = Math.floor(currentProgress * progressBarWidth);
        const progressBar = '█'.repeat(filledWidth) + '░'.repeat(progressBarWidth - filledWidth);
        console.log(`   [${progressBar}] ${Math.floor(currentProgress * 100)}%`);
        
        // Если эта страница не добавила новых уникальных результатов, завершаем сбор
        if (newUniqueCount === 0) {
          console.log(`   🔍 Нет новых уникальных результатов, останавливаем сбор`);
          break;
        }
      } else {
        console.log(`   ⚠️ Страница ${page + PAGE_OFFSET}: нет результатов`);
      }
    } catch (error) {
      console.error(`   ❌ Ошибка при обработке страницы ${page + PAGE_OFFSET}: ${error.message}`);
      
      // Дополнительная информация об ошибке для диагностики
      if (CONFIG.debug) {
        if (error.response) {
          console.error(`   ℹ️ Детали ошибки API:`);
          console.error(`     - Код: ${error.code || 'N/A'}`);
          console.error(`     - Статус: ${error.response?.status || 'N/A'}`);
          console.error(`     - Сообщение: ${error.response?.data || error.message}`);
        } else if (error.stack) {
          console.error(`   ℹ️ Стек вызовов: ${error.stack.split('\n')[0]}`);
        }
      }
      
      // Добавляем пустой объект результатов, чтобы не прерывать сбор данных
      const errorObject = {
        error: true,
        message: error.message,
        code: error.code || 'unknown',
        page: page + PAGE_OFFSET,
        timestamp: new Date().toISOString()
      };
      
      // Проверяем, связана ли ошибка с отсутствием результатов поиска (код 15)
      if (error.message.includes('Для заданного поискового запроса отсутствуют результаты поиска') || 
          error.message.includes('отсутствуют результаты') ||
          error.message.includes('code="15"')) {
        console.log(`   ⚠️ Предупреждение: Для запроса "${query}" отсутствуют результаты поиска. Пропускаем...`);
        // Добавляем пустой результат и продолжаем работу
        uniqueResults.push({
          empty: true,
          reason: 'Нет результатов для запроса',
          query: query,
          engine: engine
        });
        break; // Прекращаем обработку страниц для этого запроса
      }
      // Если это первая страница и ошибка критичная (не связанная с отсутствием результатов), пробрасываем её дальше
      else if (page === (engine === 'YANDEX' ? 0 : 1) && uniqueResults.length === 0 && error.code !== 111) {
        throw new Error(`Критическая ошибка при получении данных: ${error.message}`);
      }
    }
    
    // Переходим к следующей странице
    page++;
    
    // Пауза между запросами для избежания блокировок
    if (page < CONFIG.collection.maxPages) {
      await new Promise(resolve => setTimeout(resolve, CONFIG.collection.delayBetweenRequests));
    }
  }
  
  // Вычисляем общее время выполнения
  const totalTime = Math.floor((Date.now() - startTime) / 1000);
  const minutes = Math.floor(totalTime / 60);
  const seconds = totalTime % 60;
  const timeStr = minutes > 0 ? `${minutes}м ${seconds}с` : `${seconds}с`;
  
  // Статистика
  console.log(`\n📊 Собрано ${uniqueResults.length} уникальных результатов за ${page} страниц для запроса "${query}" в городе ${city.name}`);
  console.log(`⏱️ Время выполнения: ${timeStr}`);
  
  return uniqueResults;
};

/**
 * Главная функция запуска сбора данных
 */
const main = async () => {
  // Инициализируем allResults до try-catch блока, чтобы она была доступна и в блоке catch
  // Структура для хранения всех результатов
  const allResults = {
    timestamp: new Date().toISOString(),
    totalKeywords: 0,  // Будет обновлено позже
    totalCities: 0,    // Будет обновлено позже
    citiesData: {}
  };
  
  try {
    console.log('🚀 Запуск сбора данных для 100 ключевых фраз по крупным городам');
    
    // Создаем директорию для результатов
    fs.ensureDirSync(CONFIG.resultsDir);
    fs.ensureDirSync(CONFIG.intermediateDir);
    
    // Инициализируем API клиент
    const api = new XmlRiverApi({
      userId: process.env.XMLRIVER_USER_ID,
      apiKey: process.env.XMLRIVER_API_KEY,
      resultsDir: CONFIG.resultsDir,
      maxThreads: CONFIG.collection.maxThreads || 10,
      maxRetries: CONFIG.connection.retries,
      requestTimeout: CONFIG.connection.timeout,
      debug: CONFIG.debug
    });
    
    // Загружаем список ключевых фраз
    const keywords = await readKeywordsFromFile(CONFIG.keywordsFile);
    
    // Преобразуем список городов в массив объектов
    const cities = Object.entries(MAJOR_CITIES).map(([name, code]) => ({ name, code }));
    
    // Проверяем баланс перед началом сбора данных
    try {
      const balance = await api.getBalance();
      console.log(`💰 Текущий баланс: ${balance}`);
    } catch (error) {
      console.error(`❌ Ошибка при получении баланса: ${error.message}`);
    }
    
    // Проверяем наличие промежуточных результатов для возможного возобновления
    const resumeFile = 'serp_collection_progress';
    let resumeData = loadIntermediateResults(resumeFile);
    let startFromCity = 0;
    let startFromKeyword = 0;
    
    // Если есть данные для возобновления, используем их
    if (resumeData) {
      console.log(`🔄 Найдены промежуточные результаты, возобновление сбора данных`);
      
      // Определяем позицию для возобновления
      startFromCity = resumeData.currentCityIndex || 0;
      startFromKeyword = resumeData.currentKeywordIndex || 0;
      
      console.log(`✔️ Возобновление с города ${cities[startFromCity].name} (индекс ${startFromCity}) и ключевого слова "${keywords[startFromKeyword]}" (индекс ${startFromKeyword})`);
    }
    
    // Общий таймер для всего процесса сбора данных
    const globalStartTime = Date.now();
    
    // Обновляем информацию в структуре для хранения всех результатов
    allResults.totalKeywords = keywords.length;
    allResults.totalCities = cities.length;
    
    // Для каждого города, начиная с позиции возобновления
    for (let cityIndex = startFromCity; cityIndex < cities.length; cityIndex++) {
      const city = cities[cityIndex];
      console.log(`\n🏙️ Обработка города: ${city.name} (код ${city.code}) - ${cityIndex + 1}/${cities.length}`);
      
      // Инициализируем структуру данных для города, если она еще не существует
      if (!allResults.citiesData[city.name]) {
        allResults.citiesData[city.name] = {
          code: city.code,
          keywords: {},
          totalKeywords: keywords.length,
          timestamp: new Date().toISOString()
        };
      }
      
      // Для каждого ключевого слова, начиная с позиции возобновления
      // При переходе к новому городу начинаем с первого ключа
      const keywordStartIndex = (cityIndex === startFromCity) ? startFromKeyword : 0;
      
      for (let keywordIndex = keywordStartIndex; keywordIndex < keywords.length; keywordIndex++) {
        const keyword = keywords[keywordIndex];
        const totalProcessed = cityIndex * keywords.length + keywordIndex + 1;
        const total = cities.length * keywords.length;
        const percentComplete = ((totalProcessed / total) * 100).toFixed(1);
        
        // Рассчитываем общее оставшееся время на основе текущей скорости
        const elapsedTime = (Date.now() - globalStartTime) / 1000; // в секундах
        const globalProgress = totalProcessed / total;
        
        // Оценка общего оставшегося времени
        let remainingGlobalTime = 0;
        if (globalProgress > 0 && globalProgress < 1) {
          remainingGlobalTime = (elapsedTime / globalProgress) * (1 - globalProgress);
        }
        
        // Рассчитываем среднюю скорость обработки ключевых слов
        const avgKeywordsPerMin = totalProcessed / (elapsedTime / 60 || 0.1);
        
        // Форматируем оставшееся время
        const remainingHours = Math.floor(remainingGlobalTime / 3600);
        const remainingMinutes = Math.floor((remainingGlobalTime % 3600) / 60);
        const remainingSeconds = Math.floor(remainingGlobalTime % 60);
        
        let timeStr = '';
        if (remainingHours > 0) {
          timeStr = `${remainingHours}ч ${remainingMinutes}м ${remainingSeconds}с`;
        } else if (remainingMinutes > 0) {
          timeStr = `${remainingMinutes}м ${remainingSeconds}с`;
        } else {
          timeStr = `${remainingSeconds}с`;
        }
        
        // Создаем строку с глобальным прогресс-баром
        const progressBarWidth = 20;
        const filledWidth = Math.floor(globalProgress * progressBarWidth);
        const globalProgressBar = '█'.repeat(filledWidth) + '░'.repeat(progressBarWidth - filledWidth);
        
        console.log(`\n🔍 Обработка ключа "${keyword}" (${keywordIndex + 1}/${keywords.length}) - Общий прогресс: ${percentComplete}%`);
        
        // Добавляем вывод общего таймера и прогресса
        console.log(`📊 ОБЩИЙ ПРОГРЕСС: ${Math.floor(globalProgress * 100)}% | Обработано: ${totalProcessed}/${total} ключей`);
        console.log(`⏱️ Скорость: ${avgKeywordsPerMin.toFixed(1)} ключей/мин | Общее оставшееся время: ~${timeStr}`);
        console.log(`[общий прогресс: ${globalProgressBar}] ${Math.floor(globalProgress * 100)}%`);
        
        // Проверяем, есть ли уже собранные данные для этого ключа в текущем городе
        if (!allResults.citiesData[city.name].keywords[keyword]) {
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
            },
            timestamp: new Date().toISOString()
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
        } else {
          console.log(`♻️ Данные для ключа "${keyword}" в городе ${city.name} уже собраны, пропускаем`);
        }
        
        // Сохраняем прогресс сбора данных после каждого ключевого слова
        // Таким образом, при следующем запуске мы начнем с следующего ключевого слова
        saveResults(
          {
            timestamp: new Date().toISOString(),
            progress: {
              total: total,
              processed: totalProcessed,
              percent: percentComplete
            },
            currentCityIndex: cityIndex,
            currentKeywordIndex: keywordIndex + 1, // Сохраняем следующий индекс, чтобы начать с него
            cities: cities.map(c => c.name),
            keywords: keywords.length
          },
          resumeFile,
          true
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
      if (cityIndex < cities.length - 1) {
        console.log(`⏳ Пауза ${CONFIG.collection.delayBetweenCities / 1000} секунд перед следующим городом...`);
        await new Promise(resolve => setTimeout(resolve, CONFIG.collection.delayBetweenCities));
      }
    }
    
    // Сохраняем итоговые результаты
    const finalReportPath = saveResults(
      allResults,
      `full_report_${new Date().toISOString().replace(/:/g, '-')}`
    );
    
    // Собираем все уникальные URL в один массив
    console.log(`🔍 Сбор всех уникальных URL...`);
    const allUniqueUrls = new Set();
    const urlData = {
      yandex: [],
      google: []
    };
    
    // Проходим по всем городам и ключевым словам, собирая уникальные URL
    Object.values(allResults.citiesData).forEach(cityData => {
      Object.entries(cityData.keywords).forEach(([keyword, data]) => {
        // Добавляем URL из Яндекса
        data.yandex?.results?.forEach(result => {
          if (result.url && !allUniqueUrls.has(result.url)) {
            allUniqueUrls.add(result.url);
            urlData.yandex.push({
              url: result.url,
              title: result.title || '',
              snippet: result.snippet || '',
              keyword,
              city: cityData.name,
              engine: 'yandex'
            });
          }
        });
        
        // Добавляем URL из Google
        data.google?.results?.forEach(result => {
          if (result.url && !allUniqueUrls.has(result.url)) {
            allUniqueUrls.add(result.url);
            urlData.google.push({
              url: result.url,
              title: result.title || '',
              snippet: result.snippet || '',
              keyword,
              city: cityData.name,
              engine: 'google'
            });
          }
        });
      });
    });
    
    // Сохраняем все уникальные URL в один файл
    const allUrlsPath = saveResults(
      {
        timestamp: new Date().toISOString(),
        totalUniqueUrls: allUniqueUrls.size,
        yandexUrls: urlData.yandex.length,
        googleUrls: urlData.google.length,
        yandex: urlData.yandex,
        google: urlData.google,
        allUrls: [...urlData.yandex, ...urlData.google]
      },
      `all_unique_urls_${new Date().toISOString().replace(/:/g, '-')}`
    );
    
    // Сохраняем также в CSV формате для удобства импорта
    try {
      const csvPath = path.join(CONFIG.resultsDir, `all_unique_urls_${new Date().toISOString().replace(/:/g, '-')}.csv`);
      const csvHeader = 'URL,Title,Snippet,Keyword,City,Engine\n';
      let csvContent = csvHeader;
      
      // Добавляем все URL в CSV
      [...urlData.yandex, ...urlData.google].forEach(item => {
        // Экранируем кавычки и запятые в полях
        const escapeCSV = (field) => {
          if (!field) return '';
          const escaped = field.toString().replace(/"/g, '""');
          return `"${escaped}"`;
        };
        
        csvContent += `${escapeCSV(item.url)},${escapeCSV(item.title)},${escapeCSV(item.snippet)},${escapeCSV(item.keyword)},${escapeCSV(item.city)},${escapeCSV(item.engine)}\n`;
      });
      
      fs.writeFileSync(csvPath, csvContent);
      console.log(`💾 Сохранен CSV файл со всеми уникальными URL: ${csvPath}`);
    } catch (e) {
      console.error(`⚠️ Ошибка при сохранении CSV: ${e.message}`);
    }
    
    // Удаляем файл прогресса, так как процесс завершен
    const progressFilePath = path.join(CONFIG.intermediateDir, `${resumeFile}.json`);
    if (fs.existsSync(progressFilePath)) {
      try {
        fs.unlinkSync(progressFilePath);
        console.log(`🚮 Файл прогресса удален: ${progressFilePath}`);
      } catch (e) {
        console.error(`⚠️ Не удалось удалить файл прогресса: ${e.message}`);
      }
    }
    
    // Рассчитываем итоговую статистику
    const globalEndTime = Date.now();
    const totalTimeMs = globalEndTime - globalStartTime;
    const totalTimeSec = Math.floor(totalTimeMs / 1000);
    const totalMinutes = Math.floor(totalTimeSec / 60);
    const totalSeconds = totalTimeSec % 60;
    const totalHours = Math.floor(totalMinutes / 60);
    const minutes = totalMinutes % 60;
    
    // Форматируем время
    let timeStr = '';
    if (totalHours > 0) {
      timeStr = `${totalHours}ч ${minutes}м ${totalSeconds}с`;
    } else if (minutes > 0) {
      timeStr = `${minutes}м ${totalSeconds}с`;
    } else {
      timeStr = `${totalSeconds}с`;
    }
    
    // Подсчитываем общее количество собранных URL
    let totalUrls = 0;
    let totalYandexUrls = 0;
    let totalGoogleUrls = 0;
    
    Object.values(allResults.citiesData).forEach(cityData => {
      Object.values(cityData.keywords).forEach(keywordData => {
        totalYandexUrls += keywordData.yandex?.results?.length || 0;
        totalGoogleUrls += keywordData.google?.results?.length || 0;
      });
    });
    
    totalUrls = totalYandexUrls + totalGoogleUrls;
    
    // Выводим итоговую статистику
    console.log(`\n🎉 Сбор данных завершен!`);
    console.log(`📈 Статистика:`);
    console.log(`   - Обработано городов: ${cities.length}`);
    console.log(`   - Обработано ключевых фраз: ${keywords.length}`);
    console.log(`   - Всего запросов: ${cities.length * keywords.length * 2} (Яндекс + Google)`);
    console.log(`   - Всего собрано URL: ${totalUrls} (Яндекс: ${totalYandexUrls}, Google: ${totalGoogleUrls})`);
    console.log(`⏰ Общее время выполнения: ${timeStr}`);
    console.log(`💾 Итоговый отчет: ${finalReportPath}`);
    
    // Проверяем баланс после завершения сбора данных
    try {
      const finalBalance = await api.getBalance();
      console.log(`💰 Остаток баланса: ${finalBalance}`);
    } catch (error) {
      console.log(`   ❌ Ошибка при обработке страницы 1: ${error.message}`);
      console.log(`   ℹ️ Стек вызовов: ${error}`);
      
      // Проверяем, связана ли ошибка с отсутствием результатов
      if (error.message.includes('Для заданного поискового запроса отсутствуют результаты поиска') ||
          error.message.includes('отсутствуют результаты') ||
          error.message.includes('code="15"')) {
        console.log(`   ⚠️ Для запроса "${query}" отсутствуют результаты. Пропускаем...`);
        return {
          query,
          city: city.name,
          engine,
          uniqueResults: [],
          totalResults: 0,
          timestamp: new Date().toISOString(),
          note: 'Нет результатов для данного запроса'
        };
      }
      
      // Если это другая ошибка, которую мы не умеем обрабатывать
      throw new Error(`Критическая ошибка при получении данных: ${error.message}`);
    }
    console.error(error.stack);
    
    // Сохраняем прогресс даже в случае ошибки
    if (allResults && Object.keys(allResults).length > 0) {
      console.log(`🚨 Пытаемся сохранить промежуточные результаты...`);
      saveResults(allResults, `error_recovery_${new Date().toISOString().replace(/:/g, '-')}`);
    }
  } catch (error) {
    console.error(`\n❌ Критическая ошибка: ${error.message}`);
    console.error(error.stack);
    
    // Сохраняем прогресс даже в случае ошибки
    if (allResults && Object.keys(allResults).length > 0) {
      console.log(`🚨 Пытаемся сохранить промежуточные результаты...`);
      saveResults(allResults, `error_recovery_${new Date().toISOString().replace(/:/g, '-')}`);
    }
  }
}

// Запускаем процесс, если скрипт вызван напрямую
if (require.main === module) {
  main().catch(console.error);
}

module.exports = { main, getUniqueResults };