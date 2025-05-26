/**
 * @file serp-collector-bfl-enhanced.js
 * @description Улучшенный скрипт для сбора данных по банкротству физлиц через API XMLRiver
 */

require('dotenv').config();
const { XmlRiverApi } = require('./src/xmlriver-api');
// Используем Promises для параллельного выполнения без дополнительных библиотек
const fs = require('fs-extra');
const path = require('path');

// Настройки для запросов
const TEST_QUERIES = [
  'банкротство физических лиц',
  'закон о банкротстве',
  'стоимость банкротства',
  'как стать банкротом',
  'последствия банкротства',
  'процедура банкротства',
  'МФЦ банкротство',
  'списание долгов банкротство',
  'банкротство 127 фз',
  'банкротство через суд'
];

// Полный список городов Москвы и МО
const CITIES = [
  // Москва
  { name: 'Москва', code: 213 },
  
  // Города Московской области
  { name: 'Балашиха', code: 10716 },
  { name: 'Химки', code: 10720 },
  { name: 'Подольск', code: 10741 },
  { name: 'Королёв', code: 10738 },
  { name: 'Мытищи', code: 10739 },
  { name: 'Люберцы', code: 10740 },
  { name: 'Красногорск', code: 10742 },
  { name: 'Электросталь', code: 10768 },
  { name: 'Коломна', code: 10746 }
];

// Ограничение максимального количества потоков
const MAX_CONCURRENT = 7; // Снижено с 10 до 7 для уменьшения количества ошибок

// Класс для управления параллельными задачами с ограничением количества одновременных задач
class TaskQueue {
  constructor(concurrency) {
    this.concurrency = concurrency;
    this.running = 0;
    this.queue = [];
  }

  // Добавляет задачу в очередь
  add(task) {
    return new Promise((resolve, reject) => {
      this.queue.push({ task, resolve, reject });
      this.next();
    });
  }

  // Запускает следующую задачу, если есть свободные слоты
  next() {
    if (this.running >= this.concurrency || this.queue.length === 0) {
      return;
    }

    const { task, resolve, reject } = this.queue.shift();
    this.running++;

    Promise.resolve(task())
      .then((result) => {
        this.running--;
        resolve(result);
        this.next();
      })
      .catch((error) => {
        this.running--;
        reject(error);
        this.next();
      });
  }
}

// Инициализация API
const api = new XmlRiverApi({
  userId: process.env.XMLRIVER_USER_ID,
  apiKey: process.env.XMLRIVER_API_KEY
});

// Загрузка ключевых слов из файла
function loadKeywords() {
  const keywordsPath = path.join(__dirname, 'keywords_bfl.txt');
  
  if (fs.existsSync(keywordsPath)) {
    console.log(`📄 Загрузка ключевых слов из файла: ${keywordsPath}`);
    const content = fs.readFileSync(keywordsPath, 'utf8');
    
    const keywords = content
      .split('\n')
      .map(line => line.trim())
      .filter(line => line && !line.startsWith('#'));
      
    console.log(`✅ Загружено ${keywords.length} ключевых слов`);
    return keywords;
  }
  
  console.log('⚠️ Файл с ключевыми словами не найден, использую встроенный список');
  return TEST_QUERIES;
}

// Функция сбора данных для одного запроса с повторными попытками
async function collectDataForQuery(query, cityName, cityCode, maxRetries = 3, delayMs = 2000) {
  const startTime = Date.now();
  console.log(`🔍 [${cityName}] Запрос: "${query}"`);
  
  // Функция задержки
  const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));
  
  // Функция для запроса с повторными попытками
  async function fetchWithRetry(fetchFunc, retryCount = 0) {
    try {
      return await fetchFunc();
    } catch (error) {
      // Если ошибка связана с необходимостью повторного запроса
      if (retryCount < maxRetries) {
        console.log(`⚠️ [${cityName}] Повторная попытка ${retryCount + 1}/${maxRetries} для "${query}": ${error.message}`);
        await delay(delayMs); // Ждем перед повторной попыткой
        return fetchWithRetry(fetchFunc, retryCount + 1);
      }
      
      // Если превышено максимальное количество попыток или другая ошибка, возвращаем ошибку
      throw error;
    }
  }
  
  try {
    // Результаты для возврата
    let result = {
      query,
      city: cityName,
      yandex: null,
      google: null,
      timestamp: new Date().toISOString()
    };

    // Получаем данные из Яндекса с повторными попытками
    try {
      console.log(`🔍 Получение результатов для "${query}" в ${cityName} (YANDEX)`);
      const yandexResults = await fetchWithRetry(() => api.searchYandex(query, {
        page: 0,
        groupby: 10,
        loc: cityCode,
        within: 2, // За месяц
        filter: 1
      }));
      
      const yandexCount = yandexResults && yandexResults.results ? yandexResults.results.length : 0;
      console.log(`✅ Получено ${yandexCount} результатов из YANDEX`);
      
      result.yandex = {
        uniqueResults: yandexResults.results || [],
        totalResults: yandexCount,
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      console.error(`❌ Критическая ошибка при получении результатов Яндекса: ${error.message}`);
      result.yandex = {
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
    
    // Небольшая пауза между запросами
    await delay(1000);
    
    // Получаем данные из Google с повторными попытками
    try {
      console.log(`🔍 Получение результатов для "${query}" в ${cityName} (GOOGLE)`);
      const googleResults = await fetchWithRetry(() => api.searchGoogle(query, {
        num: 100, // До 100 результатов
        gl: 'ru',
        hl: 'ru'
      }));
      
      const googleCount = googleResults && googleResults.results ? googleResults.results.length : 0;
      console.log(`✅ Получено ${googleCount} результатов из GOOGLE`);
      
      result.google = {
        uniqueResults: googleResults.results || [],
        totalResults: googleCount,
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      console.error(`❌ Критическая ошибка при получении результатов Google: ${error.message}`);
      result.google = {
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
    
    const elapsedTime = ((Date.now() - startTime) / 1000).toFixed(2);
    console.log(`✅ [${cityName}] "${query}" - время выполнения: ${elapsedTime} сек.`);
    
    return result;
  } catch (error) {
    const elapsedTime = ((Date.now() - startTime) / 1000).toFixed(2);
    console.error(`❌ [${cityName}] Ошибка для "${query}" после ${maxRetries} попыток: ${error.message}`);
    return { 
      query, 
      cityName, 
      error: error.message,
      elapsedTime: parseFloat(elapsedTime),
      success: false
    };
  }
}

// Создаем директории для результатов
const RESULTS_DIR = path.join(__dirname, 'data', 'results', 'bfl');
const INTERMEDIATE_DIR = path.join(RESULTS_DIR, 'intermediate');
const PROGRESS_FILE = path.join(RESULTS_DIR, 'progress.json');

// Сохраняет результаты в файл
function saveResults(data, fileName, isIntermediate = false) {
  try {
    const targetDir = isIntermediate ? INTERMEDIATE_DIR : RESULTS_DIR;
    fs.ensureDirSync(targetDir);
    
    const filePath = path.join(targetDir, `${fileName}.json`);
    fs.writeJsonSync(filePath, data, { spaces: 2 });
    console.log(`💾 ${isIntermediate ? 'Промежуточные' : 'Итоговые'} результаты сохранены в ${filePath}`);
    
    return filePath;
  } catch (error) {
    console.error(`❌ Ошибка при сохранении результатов: ${error.message}`);
    return null;
  }
}

// Сохранить прогресс выполнения
function saveProgress(progress) {
  try {
    fs.ensureDirSync(RESULTS_DIR);
    fs.writeJsonSync(PROGRESS_FILE, progress, { spaces: 2 });
    return true;
  } catch (error) {
    console.error(`❌ Ошибка при сохранении прогресса: ${error.message}`);
    return false;
  }
}

// Загрузить прогресс выполнения
function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      console.log(`📂 Загрузка сохраненного прогресса из ${PROGRESS_FILE}`);
      return fs.readJsonSync(PROGRESS_FILE);
    }
  } catch (error) {
    console.error(`❌ Ошибка при загрузке прогресса: ${error.message}`);
  }
  
  return {
    completedTasks: [],
    lastResults: null
  };
}

// Проверка, была ли задача уже выполнена
function isTaskCompleted(progress, keyword, cityName) {
  return progress.completedTasks.some(task => 
    task.keyword === keyword && task.cityName === cityName
  );
}

// Создает CSV-отчет с результатами
function createCsvReport(results, fileName) {
  try {
    console.log('📝 Создание CSV отчета...');
    fs.ensureDirSync(RESULTS_DIR);
    
    const filePath = path.join(RESULTS_DIR, `${fileName}.csv`);
    
    // CSV заголовок
    // Добавляем BOM маркер для корректного отображения кириллицы
    let csvContent = '\ufeff' + 'keyword,city,engine,position,title,url,snippet\n';
    
    // Перебираем все города
    Object.keys(results.citiesData).forEach(cityName => {
      const cityData = results.citiesData[cityName];
      
      // Перебираем все ключевые слова для города
      Object.keys(cityData.keywords).forEach(keyword => {
        const keywordData = cityData.keywords[keyword];
        
        // Добавляем данные из Yandex
        if (keywordData.yandex && keywordData.yandex.results) {
          keywordData.yandex.results.forEach((result, index) => {
            // Экранируем кавычки и запятые в полях
            const escapedTitle = result.title ? `"${result.title.replace(/"/g, '""')}"` : '';
            const escapedSnippet = result.snippet ? `"${result.snippet.replace(/"/g, '""')}"` : '';
            
            csvContent += `"${keyword}","${cityName}","YANDEX",${index + 1},"Результат #${index + 1}","${result.url}",${escapedSnippet}\n`;
          });
        }
        
        // Добавляем данные из Google
        if (keywordData.google && keywordData.google.results) {
          keywordData.google.results.forEach((result, index) => {
            // Экранируем кавычки и запятые в полях
            const escapedTitle = result.title ? `"${result.title.replace(/"/g, '""')}"` : '';
            const escapedSnippet = result.snippet ? `"${result.snippet.replace(/"/g, '""')}"` : '';
            
            csvContent += `"${keyword}","${cityName}","GOOGLE",${index + 1},"Результат #${index + 1}","${result.url}",${escapedSnippet}\n`;
          });
        }
      });
    });
    
    // Записываем в файл с указанием кодировки UTF-8
    fs.writeFileSync(filePath, csvContent, { encoding: 'utf8' });
    
    console.log(`📝 CSV-отчет сохранен в ${filePath}`);
    
    return filePath;
  } catch (error) {
    console.error(`❌ Ошибка при создании CSV-отчета: ${error.message}`);
    return null;
  }
}

// Основная функция
async function main() {
  console.log('🚀 Запуск сбора данных по банкротству физлиц с возможностью возобновления');
  
  // Создаем директории, если их нет
  fs.ensureDirSync(RESULTS_DIR);
  fs.ensureDirSync(INTERMEDIATE_DIR);
  
  // Загружаем ключевые слова из файла
  const keywords = loadKeywords();
  
  // Загружаем сохраненный прогресс
  const progress = loadProgress();
  console.log(`🔄 Загружен прогресс: ${progress.completedTasks.length} завершенных задач`);
  
  // Загружаем последние результаты, если они есть
  let allResults = progress.lastResults;
  
  // Если нет сохраненных результатов, создаем новую структуру
  if (!allResults) {
    allResults = {
      startTime: new Date().toISOString(),
      keywords: keywords,
      totalKeywords: keywords.length,
      totalCities: CITIES.length,
      citiesData: {}
    };
    
    // Инициализируем данные для всех городов
    CITIES.forEach(city => {
      allResults.citiesData[city.name] = {
        code: city.code,
        keywords: {},
        startTime: new Date().toISOString()
      };
    });
  }
  
  // Создаем очередь задач с ограничением параллельных задач
  const taskQueue = new TaskQueue(MAX_CONCURRENT);
  
  // Общее количество задач и счетчик завершенных
  const totalTasks = keywords.length * CITIES.length;
  let completedTasks = progress.completedTasks.length;
  const startTime = Date.now();
  
  console.log(`📈 Сбор данных для ${keywords.length} ключевых фраз и ${CITIES.length} городов`);
  console.log(`⚙️ Настройки сбора: максимальное количество параллельных задач: ${MAX_CONCURRENT}`);
  console.log(`🔄 Уже выполнено: ${completedTasks}/${totalTasks} задач (${((completedTasks / totalTasks) * 100).toFixed(1)}%)`);
  
  
  // Перебираем все города и запросы
  const promises = [];
  let tasksCount = 0;
  
  for (const city of CITIES) {
    for (const keyword of keywords) {
      // Проверяем, была ли эта задача уже выполнена
      if (!isTaskCompleted(progress, keyword, city.name)) {
        tasksCount++;
        // Создаем задачу и добавляем ее в очередь с ограничением параллельных задач
        promises.push(taskQueue.add(() => {
          return collectDataForQuery(keyword, city.name, city.code)
            .then(result => {
              completedTasks++;
              
              // Сохраняем данные в структуру
              if (!allResults.citiesData[city.name].keywords[keyword]) {
                allResults.citiesData[city.name].keywords[keyword] = {
                  yandex: result.yandex,
                  google: result.google,
                  timestamp: new Date().toISOString()
                };
              }
              
              // Обновляем время окончания для города
              allResults.citiesData[city.name].endTime = new Date().toISOString();
              
              // Добавляем задачу в список выполненных
              progress.completedTasks.push({
                keyword,
                cityName: city.name,
                timestamp: new Date().toISOString()
              });
              
              // Обновляем последние результаты в прогрессе
              progress.lastResults = allResults;
              
              // Сохраняем прогресс после каждой задачи
              saveProgress(progress);
              
              // Расчет прогресса и оценка оставшегося времени
              const percent = ((completedTasks / totalTasks) * 100).toFixed(1);
              const elapsedSeconds = Math.round((Date.now() - startTime) / 1000);
              const averageTimePerTask = elapsedSeconds / completedTasks;
              const remainingTasks = totalTasks - completedTasks;
              const remainingSeconds = Math.round(averageTimePerTask * remainingTasks);
              const remainingMinutes = Math.floor(remainingSeconds / 60);
              const remainingSecondsDisplay = remainingSeconds % 60;
              
              console.log(`⏱️ Прогресс: ${percent}% (${completedTasks}/${totalTasks}), Осталось: ${remainingMinutes}м ${remainingSecondsDisplay}с, Активных потоков: ${taskQueue.running}/${MAX_CONCURRENT}`);
              
              // Промежуточное сохранение результатов после каждой 5-й задачи
              if (completedTasks % 5 === 0) {
                // Обновляем время окончания
                allResults.endTime = new Date().toISOString();
                const intermediateFileName = 'intermediate_results';
                saveResults(allResults, intermediateFileName, true);
              }
              
              return result;
            });
        }));
      } else {
        console.log(`✅ Задача ${keyword} для города ${city.name} уже выполнена, пропускаем...`);
      }
    }
  }
  
  // Дожидаемся выполнения всех задач
  console.log('⌛ Ожидание завершения всех задач...');
  
  try {
    await Promise.all(promises);
    const totalSeconds = Math.round((Date.now() - startTime) / 1000);
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;
    console.log(`🎉 Все задачи выполнены! Затрачено времени: ${minutes}м ${seconds}с`);
    
    // Добавляем итоговую информацию в результаты
    allResults.endTime = new Date().toISOString();
    allResults.totalDuration = `${minutes}м ${seconds}с`;
    allResults.durationSeconds = totalSeconds;
    allResults.statistics = {
      totalTasks,
      completedTasks,
      successRate: `${((completedTasks / totalTasks) * 100).toFixed(1)}%`
    };
    
    // Сохраняем итоговые результаты
    const timestamp = new Date().toISOString().replace(/:/g, '-').replace(/\./g, '-').substring(0, 19);
    const fileName = `bfl_results_${timestamp}`;
    
    // Сохраняем JSON с полными результатами
    const jsonFilePath = saveResults(allResults, fileName);
    
    // Создаем CSV отчет
    const csvFilePath = createCsvReport(allResults, fileName);
    
    console.log(`📊 Сбор данных успешно завершен!`); 
    console.log(`💾 JSON: ${jsonFilePath}`); 
    console.log(`📝 CSV: ${csvFilePath}`); 
    console.log(`
✨ Статистика выполнения:`);
    console.log(`   📊 Всего задач: ${totalTasks}`);
    console.log(`   ✅ Выполнено: ${completedTasks}`);
    console.log(`   💸 Успешность: ${((completedTasks / totalTasks) * 100).toFixed(1)}%`);
    console.log(`   ⏱️ Время выполнения: ${minutes}м ${seconds}с`);
    
    // Если все задачи выполнены, удаляем файл прогресса
    if (completedTasks >= totalTasks) {
      try {
        if (fs.existsSync(PROGRESS_FILE)) {
          fs.removeSync(PROGRESS_FILE);
          console.log(`🗑️ Файл прогресса удален, все задачи выполнены`);
        }
      } catch (error) {
        console.error(`❌ Ошибка при удалении файла прогресса: ${error.message}`);
      }
    }
  } catch (error) {
    console.error(`💥 Ошибка при выполнении задач: ${error.message}`);
    
    // Сохраняем прогресс и частичные результаты при ошибке
    try {
      // Сохраняем прогресс, чтобы можно было продолжить позже
      if (progress && progress.completedTasks) {
        // Обновляем последние результаты в прогрессе
        progress.lastResults = allResults;
        saveProgress(progress);
        console.log(`🔄 Прогресс сохранен с ${progress.completedTasks.length} выполненными задачами. Вы можете продолжить позже.`);
      }
      
      allResults.error = error.message;
      allResults.endTime = new Date().toISOString();
      
      const timestamp = new Date().toISOString().replace(/:/g, '-').replace(/\./g, '-').substring(0, 19);
      const fileName = `bfl_results_error_${timestamp}`;
      
      // Сохраняем JSON с частичными результатами
      const jsonFilePath = saveResults(allResults, fileName);
      console.log(`💾 Частичные результаты сохранены в: ${jsonFilePath}`);
    } catch (saveError) {
      console.error(`❌ Ошибка при сохранении частичных результатов: ${saveError.message}`);
    }
  }
}

// Запускаем основную функцию
main().catch(err => {
  console.error(`❌ Ошибка выполнения скрипта:`, err);
});
