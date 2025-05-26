/**
 * Скрипт для извлечения только уникальных URL из результатов поиска
 */

const fs = require('fs-extra');
const path = require('path');

// Путь к JSON-файлу с результатами
const jsonFilePath = path.join(__dirname, 'data', 'results', 'bfl', 'bfl_results_2025-05-24T15-46-56.json');
// Путь к выходному файлу с уникальными URL
const uniqueUrlsFilePath = path.join(__dirname, 'data', 'results', 'bfl', 'russian_unique_urls.csv');
// Путь к выходному TXT-файлу с уникальными URL (только список URL)
const uniqueUrlsTxtFilePath = path.join(__dirname, 'data', 'results', 'bfl', 'russian_unique_urls.txt');

/**
 * Функция для проверки, является ли URL российским
 */
function isRussianUrl(url) {
  try {
    // Проверяем, что URL имеет допустимый формат
    if (!url || typeof url !== 'string') {
      return false;
    }
    
    // Парсим URL для получения домена
    const urlObj = new URL(url);
    const domain = urlObj.hostname.toLowerCase();
    
    // Российские домены
    const russianDomains = [
      // Основные российские домены
      '.ru', '.рф', '.su', '.москва', '.moscow', '.tatar', 
      // Популярные российские сервисы
      'yandex.ru', 'mail.ru', 'vk.com', 'ok.ru', 'ria.ru', 'rbc.ru', 'consultant.ru',
      'garant.ru', 'gosuslugi.ru', 'nalog.ru', 'pfr.gov.ru', 'mos.ru', 'sberbank.ru',
      'vtb.ru', 'alfabank.ru', 'tinkoff.ru', 'avito.ru', 'ozon.ru', 'wildberries.ru',
      'kinopoisk.ru', 'ivi.ru', 'lenta.ru', 'iz.ru', 'kp.ru', 'aif.ru', 'mk.ru',
      'gazeta.ru', 'kommersant.ru', 'vedomosti.ru', 'fontanka.ru', 'rospotrebnadzor.ru',
      'fssp.gov.ru', 'fns.gov.ru', 'arbitr.ru', 'cbr.ru', 'banki.ru', 'asv.org.ru',
      'bankrot.fedresurs.ru', 'fas.gov.ru', 'genproc.gov.ru', 'rosreestr.gov.ru'
    ];
    
    // Проверяем по зонам и известным доменам
    return russianDomains.some(russianDomain => {
      // Если это зона (начинается с точки)
      if (russianDomain.startsWith('.')) {
        return domain.endsWith(russianDomain);
      }
      // Если это конкретный домен
      return domain === russianDomain || domain.endsWith('.' + russianDomain);
    });
  } catch (error) {
    // Если URL невалидный, считаем его не российским
    return false;
  }
}

/**
 * Основная функция для извлечения уникальных URL
 */
async function extractUniqueUrls() {
  try {
    console.log('🔄 Чтение JSON файла...');
    
    // Проверяем, существует ли файл
    if (!fs.existsSync(jsonFilePath)) {
      throw new Error(`Файл ${jsonFilePath} не найден!`);
    }
    
    // Читаем JSON-файл
    const jsonData = fs.readJsonSync(jsonFilePath);
    
    // Множество для хранения уникальных URL
    const uniqueUrls = new Set();
    
    // Объект для хранения информации о URL
    const urlInfo = {};
    
    // Перебираем города
    Object.keys(jsonData.citiesData).forEach(cityName => {
      const cityData = jsonData.citiesData[cityName];
      
      // Перебираем ключевые слова для города
      Object.keys(cityData.keywords).forEach(keyword => {
        const keywordData = cityData.keywords[keyword];
        
        // Обработка результатов Яндекса
        if (keywordData.yandex && keywordData.yandex.uniqueResults) {
          keywordData.yandex.uniqueResults.forEach((result, index) => {
            if (result.url && isRussianUrl(result.url)) { // Фильтруем только российские URL
              uniqueUrls.add(result.url);
              
              // Сохраняем информацию о URL
              if (!urlInfo[result.url]) {
                urlInfo[result.url] = {
                  title: result.title || '',
                  sources: []
                };
              }
              urlInfo[result.url].sources.push(`YANDEX: ${cityName} - ${keyword}`);
            }
          });
        }
        
        // Обработка результатов Google
        if (keywordData.google && keywordData.google.uniqueResults) {
          keywordData.google.uniqueResults.forEach((result, index) => {
            if (result.url && isRussianUrl(result.url)) { // Фильтруем только российские URL
              uniqueUrls.add(result.url);
              
              // Сохраняем информацию о URL
              if (!urlInfo[result.url]) {
                urlInfo[result.url] = {
                  title: result.title || '',
                  sources: []
                };
              }
              urlInfo[result.url].sources.push(`GOOGLE: ${cityName} - ${keyword}`);
            }
          });
        }
      });
    });
    
    // Конвертируем множество в массив
    const uniqueUrlsArray = Array.from(uniqueUrls);
    
    // Создаем CSV с BOM-маркером для поддержки кириллицы
    let csvContent = '\ufeff' + 'url,title,count_sources,sources\n';
    
    // Добавляем данные в CSV
    uniqueUrlsArray.forEach(url => {
      const info = urlInfo[url];
      const sourcesCount = info.sources.length;
      const sourcesText = info.sources.join(' | ');
      
      csvContent += `"${url}","${info.title.replace(/"/g, '""')}",${sourcesCount},"${sourcesText.replace(/"/g, '""')}"\n`;
    });
    
    // Создаем простой TXT-файл только с URL (по одному на строку)
    let txtContent = uniqueUrlsArray.join('\n');
    
    // Записываем данные в файлы
    console.log(`🔄 Запись ${uniqueUrlsArray.length} уникальных URL в файлы...`);
    fs.writeFileSync(uniqueUrlsFilePath, csvContent, { encoding: 'utf8' });
    fs.writeFileSync(uniqueUrlsTxtFilePath, txtContent, { encoding: 'utf8' });
    
    console.log(`✅ CSV файл с уникальными URL создан: ${uniqueUrlsFilePath}`);
    console.log(`✅ TXT файл с уникальными URL создан: ${uniqueUrlsTxtFilePath}`);
    console.log(`📊 Всего уникальных URL: ${uniqueUrlsArray.length}`);
    
  } catch (error) {
    console.error(`❌ Ошибка при извлечении уникальных URL: ${error.message}`);
  }
}

// Запускаем извлечение уникальных URL
extractUniqueUrls();
