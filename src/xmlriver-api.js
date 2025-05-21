/**
 * @file xmlriver-api.js
 * @description Модуль для работы с API XMLRiver - получение данных из поисковой выдачи Яндекс и Google
 * @author Sergei Dyshkant (SergD)
 */

const axios = require('axios');
const fs = require('fs-extra');
const path = require('path');
const xml2js = require('xml2js');
const querystring = require('querystring');
const { promisify } = require('util');
const parseXml = promisify(xml2js.parseString);

// Базовые URL для API
const API_BASE_URL = {
  GOOGLE: 'http://xmlriver.com/search/xml',
  YANDEX: 'http://xmlriver.com/search_yandex/xml',
  BALANCE: 'http://xmlriver.com/api/get_balance/'
};

// Параметры для городов-миллионников в Яндексе (ID локаций)
const YANDEX_CITIES = {
  'Москва': 213,
  'Санкт-Петербург': 2,
  'Новосибирск': 65,
  'Екатеринбург': 54,
  'Казань': 43,
  'Нижний Новгород': 47,
  'Челябинск': 56,
  'Омск': 66,
  'Самара': 51,
  'Ростов-на-Дону': 39,
  'Уфа': 172,
  'Красноярск': 62,
  'Воронеж': 193,
  'Пермь': 50,
  'Волгоград': 38
};

// Параметры для Google
const GOOGLE_DOMAINS = {
  'google.ru': 10,
  'google.com': 1
};

const GOOGLE_COUNTRIES = {
  'Россия': 2008
};

// Коды ошибок и их описания
const ERROR_CODES = {
  1: 'Некорректный формат запроса',
  2: 'Неавторизованный доступ',
  3: 'Неверный ключ доступа',
  4: 'Неверный ID пользователя',
  5: 'Недостаточно средств',
  6: 'Превышен лимит запросов',
  7: 'Ошибка сервера',
  8: 'Запрос заблокирован',
  9: 'Неверные параметры запроса',
  15: 'Нет результатов по запросу'
};

/**
 * Класс для работы с API XMLRiver
 */
class XmlRiverApi {
  /**
   * Создает экземпляр API клиента
   * @param {Object} options - Параметры для инициализации API
   * @param {string} options.userId - ID пользователя
   * @param {string} options.apiKey - Ключ доступа к API
   * @param {string} options.resultsDir - Директория для сохранения результатов
   * @param {number} options.maxThreads - Максимальное количество одновременных потоков (макс 10)
   * @param {number} options.maxRetries - Максимальное количество повторных попыток
   * @param {number} options.requestTimeout - Таймаут запроса в миллисекундах (макс 60000)
   * @param {boolean} options.useDelayedMode - Использовать режим отложенных ответов
   * @param {boolean} options.debug - Режим отладки
   */
  constructor(options = {}) {
    this.userId = options.userId;
    this.apiKey = options.apiKey;
    this.resultsDir = options.resultsDir || path.join(__dirname, '..', 'data', 'results');
    this.maxThreads = Math.min(options.maxThreads || 10, 10); // Максимум 10 потоков
    this.maxRetries = options.maxRetries || 3;
    this.requestTimeout = Math.min(options.requestTimeout || 60000, 60000); // Максимум 60 секунд
    this.useDelayedMode = options.useDelayedMode || false;
    this.debug = options.debug || false;
    
    // Для отслеживания запросов
    this.activeRequests = 0;
    this.requestQueue = [];
    this.delayedRequests = new Map();
    
    // Создаем директорию для результатов
    fs.ensureDirSync(this.resultsDir);
    
    // Валидация необходимых параметров
    if (!this.userId || !this.apiKey) {
      throw new Error('Необходимо указать ID пользователя и API ключ');
    }
    
    // Логирование инициализации
    this.log(`API XMLRiver инициализирован. Потоков: ${this.maxThreads}`);
  }
  
  /**
   * Получение баланса аккаунта
   * @returns {Promise<number>} - Баланс аккаунта
   */
  async getBalance() {
    try {
      const url = `${API_BASE_URL.BALANCE}?user=${this.userId}&key=${this.apiKey}`;
      const response = await axios.get(url, { timeout: this.requestTimeout });
      
      // Проверяем формат ответа
      if (typeof response.data === 'object') {
        // Для ответа в формате JSON
        if (response.data.error) {
          throw new Error(`Ошибка получения баланса: ${response.data.error}`);
        }
        return response.data.balance || parseFloat(response.data.toString());
      } else if (typeof response.data === 'string') {
        // Для текстового ответа
        if (response.data.startsWith('ERROR')) {
          const errorCode = parseInt(response.data.split(' ')[1]);
          throw new Error(`Ошибка получения баланса: ${ERROR_CODES[errorCode] || response.data}`);
        }
        return parseFloat(response.data);
      } else {
        throw new Error('Неизвестный формат ответа при получении баланса');
      }
    } catch (error) {
      this.log(`Ошибка при получении баланса: ${error.message}`, 'error');
      throw error;
    }
  }
  
  /**
   * Выполняет запрос к API Google
   * @param {string} query - Поисковый запрос
   * @param {Object} options - Дополнительные параметры запроса
   * @returns {Promise<Object>} - Результаты поиска
   */
  async searchGoogle(query, options = {}) {
    return this.performSearch(query, 'GOOGLE', options);
  }
  
  /**
   * Выполняет запрос к API Яндекса
   * @param {string} query - Поисковый запрос
   * @param {Object} options - Дополнительные параметры запроса
   * @returns {Promise<Object>} - Результаты поиска
   */
  async searchYandex(query, options = {}) {
    return this.performSearch(query, 'YANDEX', options);
  }
  
  /**
   * Выполняет поисковый запрос к выбранному API
   * @param {string} query - Поисковый запрос
   * @param {string} engine - Поисковая система (GOOGLE или YANDEX)
   * @param {Object} options - Дополнительные параметры запроса
   * @returns {Promise<Object>} - Результаты поиска
   */
  async performSearch(query, engine, options = {}) {
    // Формируем безопасный запрос
    const safeQuery = query.replace(/&/g, '%26');
    
    // Базовые параметры запроса
    const params = {
      user: this.userId,
      key: this.apiKey,
      query: safeQuery,
      groupby: options.groupby || 100, // По умолчанию 100 результатов
      device: options.device || 'desktop'
    };
    
    // Добавляем параметр отложенного ответа, если включен соответствующий режим
    if (this.useDelayedMode) {
      params.delayed = 1;
    }
    
    // Специфичные параметры для Google
    if (engine === 'GOOGLE') {
      if (options.domain) params.domain = options.domain;
      if (options.country) params.country = options.country;
      if (options.lr) params.lr = options.lr;
      if (options.page !== undefined) {
        // Используем переданный номер страницы
        params.page = options.page;
      } else if (options.p !== undefined) {
        // Совместимость с p: преобразуем в page
        params.page = options.p - 1;
      }
    }
    
    // Специфичные параметры для Яндекса
    if (engine === 'YANDEX') {
      if (options.loc) params.loc = options.loc;
      if (options.lr) params.lr = options.lr;
      
      // Пагинация: в Яндексе страницы нумеруются с нуля
      if (options.page !== undefined) {
        // Используем переданный номер страницы
        params.page = options.page;
      } else if (options.p !== undefined) {
        // Совместимость с p: преобразуем в page (с учетом что страницы в Яндексе с нуля)
        params.page = options.p - 1;
      }
      
      // Дополнительные параметры из документации
      if (options.filter !== undefined) params.filter = options.filter;
      if (options.highlights !== undefined) params.highlights = options.highlights;
      if (options.within !== undefined) params.within = options.within;
      if (options.lang !== undefined) params.lang = options.lang;
      if (options.domain !== undefined) params.domain = options.domain;
    }
    
    // Формируем URL запроса
    const apiUrl = API_BASE_URL[engine];
    const url = `${apiUrl}?${querystring.stringify(params)}`;
    
    // Логируем запрос
    this.log(`Запрос ${engine}: ${safeQuery}`, 'info');
    
    // Проверяем доступность слотов для запросов
    await this.waitForAvailableSlot();
    
    try {
      // Увеличиваем счетчик активных запросов
      this.activeRequests++;
      
      // Выполняем запрос
      const response = await axios.get(url, { 
        timeout: this.requestTimeout,
        responseType: 'text',
        headers: {
          'Accept': 'application/json, text/plain, */*',
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
      });
      
      // Проверяем, если ответ пустой
      if (!response.data || response.data.trim() === '') {
        throw new Error(`Пустой ответ от API ${engine}`);
      }
      
      // Дополнительное логирование в режиме отладки
      if (this.debug) {
        // Выводим только начало ответа для диагностики
        const preview = response.data.substring(0, 200) + '...';
        this.log(`Ответ от API ${engine}: ${preview}`, 'info');
      }
      
      // Обрабатываем ответ в зависимости от режима
      if (this.useDelayedMode) {
        return this.handleDelayedResponse(response.data, engine, query, options);
      } else {
        return this.processResponse(response.data, engine, query, options);
      }
    } catch (error) {
      this.log(`Ошибка запроса ${engine} для "${safeQuery}": ${error.message}`, 'error');
      
      // Пробуем повторить запрос при необходимости
      if (options.retryCount === undefined) {
        options.retryCount = 0;
      }
      
      if (options.retryCount < this.maxRetries) {
        options.retryCount++;
        this.log(`Повторная попытка ${options.retryCount}/${this.maxRetries} для "${safeQuery}"`, 'warn');
        
        // Делаем паузу перед повторной попыткой
        await new Promise(resolve => setTimeout(resolve, 2000 * options.retryCount));
        
        return this.performSearch(query, engine, options);
      } else {
        throw new Error(`Превышено количество попыток для запроса "${safeQuery}"`);
      }
    } finally {
      // Уменьшаем счетчик активных запросов
      this.activeRequests--;
      
      // Запускаем следующий запрос из очереди
      this.processNextRequest();
    }
  }
  
  /**
   * Обрабатывает ответ от API
   * @param {string} xmlContent - XML ответ от API
   * @param {string} engine - Поисковая система (GOOGLE или YANDEX)
   * @param {string} query - Поисковый запрос
   * @param {Object} options - Параметры запроса
   * @returns {Promise<Object>} - Обработанные результаты
   */
  async processResponse(xmlContent, engine, query, options) {
    try {
      // Сохраняем сырой XML при необходимости для отладки
      if (this.debug) {
        await this.saveRawResponse(query, engine, xmlContent, options);
      }
      
      // Парсим XML
      const parsedXml = await parseXml(xmlContent, { trim: true, explicitArray: false });
      
      // Проверяем на наличие ошибок в ответе
      if (parsedXml.yandexsearch && parsedXml.yandexsearch.response && parsedXml.yandexsearch.response.error) {
        const errorMsg = parsedXml.yandexsearch.response.error._ || parsedXml.yandexsearch.response.error;
        throw new Error(`Ошибка API: ${errorMsg}`);
      }
      
      // Извлекаем результаты в зависимости от поисковой системы
      let results = [];
      let totalResults = 0;
      let pageInfo = { current: 1, total: 1 };
      
      if (engine === 'GOOGLE') {
        results = this.extractGoogleResults(parsedXml);
        
        // Получаем общее количество результатов и информацию о страницах
        if (parsedXml.yandexsearch && parsedXml.yandexsearch.response) {
          const found = parsedXml.yandexsearch.response.found;
          totalResults = found ? parseInt(found._) || parseInt(found) || 0 : 0;
          
          // Обрабатываем информацию о страницах
          const grouping = parsedXml.yandexsearch.response.results.grouping;
          if (grouping && grouping.page) {
            pageInfo.current = parseInt(grouping.page._) || 1;
            pageInfo.from = parseInt(grouping.page.$.first) || 1;
            pageInfo.to = parseInt(grouping.page.$.last) || results.length;
          }
        }
      } else {
        results = this.extractYandexResults(parsedXml);
        
        // Получаем общее количество результатов и информацию о страницах для Яндекса
        if (parsedXml.yandexsearch && parsedXml.yandexsearch.response) {
          const found = parsedXml.yandexsearch.response.found;
          totalResults = found ? parseInt(found._) || parseInt(found) || 0 : 0;
          
          // Обрабатываем информацию о страницах для Яндекса
          const grouping = parsedXml.yandexsearch.response.results.grouping;
          if (grouping && grouping.page) {
            pageInfo.current = parseInt(grouping.page._) || 1;
            pageInfo.from = parseInt(grouping.page.$.first) || 1;
            pageInfo.to = parseInt(grouping.page.$.last) || results.length;
          }
        }
      }
      
      // Если результаты не найдены, но есть XML, пробуем извлечь URL из сырого XML
      if (results.length === 0 && xmlContent.includes('<url>')) {
        this.log('Результаты не найдены в структурированных данных, пробуем извлечь URL из сырого XML', 'warn');
        
        const urls = [];
        const urlMatches = xmlContent.match(/<url>([^<]+)<\/url>/g);
        
        if (urlMatches && urlMatches.length > 0) {
          urlMatches.forEach((match, index) => {
            const url = match.replace(/<url>/, '').replace(/<\/url>/, '');
            urls.push({
              url: url,
              title: `Результат #${index + 1}`,
              snippet: '',
              position: index + 1
            });
          });
          
          results = urls;
          this.log(`Извлечено ${urls.length} URL из сырого XML`, 'info');
        }
      }
      
      // Сохраняем результаты при необходимости
      if (options.saveResults !== false) {
        await this.saveResults(query, engine, results, options);
      }
      
      // Формируем и возвращаем результат
      return {
        query,
        engine,
        totalResults,
        pageInfo,
        results,
        options,
        timestamp: new Date().toISOString(),
        raw: this.debug ? xmlContent : undefined
      };
    } catch (error) {
      this.log(`Ошибка при обработке ответа от ${engine}: ${error.message}`, 'error');
      
      // Пытаемся извлечь URL из сырого XML, если не удалось разобрать ответ
      if (xmlContent && xmlContent.includes('<url>')) {
        this.log('Пробуем извлечь URL из сырого XML после ошибки', 'warn');
        
        const urls = [];
        const urlMatches = xmlContent.match(/<url>([^<]+)<\/url>/g);
        
        if (urlMatches && urlMatches.length > 0) {
          urlMatches.forEach((match, index) => {
            const url = match.replace(/<url>/, '').replace(/<\/url>/, '');
            urls.push({
              url: url,
              title: `Результат #${index + 1}`,
              snippet: '',
              position: index + 1
            });
          });
          
          this.log(`Извлечено ${urls.length} URL из сырого XML`, 'info');
          
          return {
            query,
            engine,
            totalResults: urls.length,
            pageInfo: { current: 1, total: 1 },
            results: urls,
            options,
            timestamp: new Date().toISOString(),
            raw: this.debug ? xmlContent : undefined,
            warning: 'Данные извлечены из сырого XML из-за ошибки парсинга'
          };
        }
      }
      
      throw error;
    }
  }

  /**
   * Сохраняет сырой XML-ответ в файл для отладки
   * @param {string} query - Поисковый запрос
   * @param {string} engine - Поисковая система
   * @param {string} xmlContent - XML-ответ от API
   * @param {Object} options - Параметры запроса
   */
  async saveRawResponse(query, engine, xmlContent, options) {
    // Формируем имя файла на основе параметров запроса
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const safeName = query.replace(/[^a-zA-Zа-яА-Я0-9]/g, '_').substring(0, 50);
    const page = options.page !== undefined ? `_page${options.page}` : '';
    const filename = `${engine.toLowerCase()}_${safeName}${page}_${timestamp}.xml`;
    
    // Создаем директорию для сырых ответов, если не существует
    const rawDir = path.join(this.resultsDir, 'raw');
    await fs.ensureDir(rawDir);
    
    // Записываем XML в файл
    const filePath = path.join(rawDir, filename);
    await fs.writeFile(filePath, xmlContent, 'utf8');
    
    this.log(`Сохранен сырой XML-ответ: ${filename}`, 'info');
  }

  /**
   * Обрабатывает ответ от API в режиме отложенного ответа
   * @param {string} response - Ответ от API
   * @param {string} engine - Поисковая система
   * @param {string} query - Поисковый запрос
   * @param {Object} options - Параметры запроса
   * @returns {Promise<Object>} - Результаты поиска
   */
  async handleDelayedResponse(response, engine, query, options) {
    // Проверяем, получили ли мы ID запроса или ошибку
    if (response.startsWith('ERROR')) {
      const errorCode = parseInt(response.split(' ')[1]);
      throw new Error(`Ошибка API: ${ERROR_CODES[errorCode] || response}`);
    }
    
    // Получаем ID запроса
    const requestId = parseInt(response.trim());
    
    if (isNaN(requestId)) {
      throw new Error(`Неверный ID запроса: ${response}`);
    }
    
    // Сохраняем информацию о запросе
    this.delayedRequests.set(requestId, {
      query,
      engine,
      options,
      timestamp: Date.now()
    });
    
    this.log(`Запрос принят в обработку, ID: ${requestId}`, 'info');
    
    // Возвращаем ID запроса
    return { requestId, status: 'pending', query };
  }
  
  /**
   * Получает результаты отложенного запроса
   * @param {number} requestId - ID запроса
   * @returns {Promise<Object>} - Результаты поиска
   */
  async getDelayedResults(requestId) {
    const url = `${API_BASE_URL.GOOGLE}?req_id=${requestId}`;
    
    try {
      const response = await axios.get(url, { timeout: this.requestTimeout });
      
      // Проверяем статус
      if (response.data === 'WAIT') {
        return { requestId, status: 'waiting' };
      }
      
      // Проверяем на ошибки
      if (response.data.startsWith('ERROR')) {
        const errorMsg = response.data.includes('Bad request id') 
          ? 'Неверный ID запроса или результаты более не доступны' 
          : `Ошибка API: ${response.data}`;
        
        throw new Error(errorMsg);
      }
      
      // Получаем информацию о запросе
      const requestInfo = this.delayedRequests.get(requestId);
      
      if (!requestInfo) {
        throw new Error('Запрос не найден в локальной истории');
      }
      
      // Обрабатываем результаты
      const results = await this.processResponse(
        response.data, 
        requestInfo.engine, 
        requestInfo.query, 
        requestInfo.options
      );
      
      // Удаляем запрос из истории
      this.delayedRequests.delete(requestId);
      
      return results;
    } catch (error) {
      this.log(`Ошибка при получении результатов запроса ${requestId}: ${error.message}`, 'error');
      throw error;
    }
  }
  
  /**
   * Проверяет все отложенные запросы
   * @returns {Promise<Array>} - Массив завершенных запросов
   */
  async checkAllDelayedRequests() {
    const results = [];
    const pendingRequests = [];
    
    // Проходим по всем отложенным запросам
    for (const [requestId, requestInfo] of this.delayedRequests.entries()) {
      try {
        const result = await this.getDelayedResults(requestId);
        
        if (result.status === 'waiting') {
          pendingRequests.push(requestId);
        } else {
          results.push(result);
        }
      } catch (error) {
        this.log(`Ошибка при проверке запроса ${requestId}: ${error.message}`, 'error');
        
        // Если запрос старше 10 минут, удаляем его
        if (Date.now() - requestInfo.timestamp > 10 * 60 * 1000) {
          this.delayedRequests.delete(requestId);
          this.log(`Запрос ${requestId} удален из-за истечения времени ожидания`, 'warn');
        }
      }
    }
    
    this.log(`Проверено ${results.length + pendingRequests.length} запросов. Готово: ${results.length}, в ожидании: ${pendingRequests.length}`);
    
    return results;
  }
  
  /**
   * Обрабатывает ответ от API
   * @param {string} response - Ответ от API
   * @param {string} engine - Поисковая система
      
      throw new Error(`${errorDescription} (${errorCode})`);
        if (this.debug) {
          console.log("Структура ответа:", Object.keys(result));
        }
        
        // Извлекаем результаты в зависимости от поисковой системы
        let searchResults = [];
        let total = 0;
        
        if (engine === 'GOOGLE') {
          searchResults = this.extractGoogleResults(result);
          total = parseInt(result?.response?.results_count?.[0] || 0);
        } else if (engine === 'YANDEX') {
          searchResults = this.extractYandexResults(result);
          total = parseInt(result?.yandexsearch?.response?.[0]?.results_count?.[0] || 0);
        }
        
        // Сохраняем результаты
        this.saveResults(query, engine, searchResults, options);
        
        this.log(`Получено ${searchResults.length} результатов для "${query}" (${engine})`, 'info');
        
        // Возвращаем структурированные результаты
        return {
          query,
          engine,
          results: searchResults,
          total,
          options,
          timestamp: new Date().toISOString()
        };
      } catch (xmlParseError) {
        this.log(`Ошибка при парсинге XML: ${xmlParseError.message}`, 'error');
        
        // Если не удалось распарсить XML, возвращаем пустой результат
        return {
          query,
          engine,
          results: [],
          total: 0,
          options,
          timestamp: new Date().toISOString(),
          error: xmlParseError.message
        };
      }
    } catch (error) {
      this.log(`Ошибка при обработке ответа для "${query}": ${error.message}`, 'error');
      throw error;
    }
  }
  
  /**
   * Извлекает результаты из ответа Google
   * @param {Object} parsedXml - Распарсенный XML-ответ
   * @returns {Array} - Результаты поиска
   */
  extractGoogleResults(parsedXml) {
    try {
      const results = [];
      const items = parsedXml?.response?.results?.[0]?.result || [];
      
      for (const item of items) {
        results.push({
          title: item.title?.[0] || '',
          url: item.url?.[0] || '',
          displayUrl: item.display_url?.[0] || '',
          snippet: item.snippet?.[0] || '',
          position: parseInt(item.position?.[0] || 0),
          domain: this.extractDomain(item.url?.[0] || '')
        });
      }
      
      return results;
    } catch (error) {
      this.log(`Ошибка при извлечении результатов Google: ${error.message}`, 'error');
      return [];
    }
  }
  
  /**
   * Извлекает результаты из ответа Яндекса
   * @param {Object} parsedXml - Распарсенный XML-ответ
   * @returns {Array} - Результаты поиска
   */
  extractYandexResults(parsedXml) {
    try {
      const results = [];
      
      // Проверяем структуру ответа
      if (parsedXml.yandexsearch) {
        // XML прямо от API Yandex
        const groups = parsedXml.yandexsearch?.response?.[0]?.results?.[0]?.grouping?.[0]?.group || [];
        
        for (const group of groups) {
          const doc = group.doc?.[0];
          
          if (doc) {
            results.push({
              title: doc.title?.[0] || '',
              url: doc.url?.[0] || '',
              snippet: doc.passages?.[0]?.passage?.[0] || '',
              domain: doc.domain?.[0] || '',
              position: parseInt(group.doccount?.[0] || 0)
            });
          }
        }
      } else {
        // Стандартный формат нашего модуля
        const items = parsedXml?.response?.results?.[0]?.grouping?.[0]?.group || [];
        
        for (const group of items) {
          const doc = group.doc?.[0];
          
          if (doc) {
            results.push({
              title: doc.title?.[0] || '',
              url: doc.url?.[0] || '',
              snippet: doc.passages?.[0]?.passage?.[0] || '',
              domain: doc.domain?.[0] || '',
              position: parseInt(group.doccount?.[0] || 0)
            });
          }
        }
      }
      
      return results;
    } catch (error) {
      this.log(`Ошибка при извлечении результатов Яндекса: ${error.message}`, 'error');
      return [];
    }
  }
  
  /**
   * Извлекает домен из URL
   * @param {string} url - URL для извлечения домена
   * @returns {string} - Домен
   */
  extractDomain(url) {
    try {
      const hostname = new URL(url).hostname;
      return hostname.replace(/^www\./, '');
    } catch (error) {
      return '';
    }
  }
  
  /**
   * Сохраняет результаты в файл
   * @param {string} query - Поисковый запрос
   * @param {string} engine - Поисковая система
   * @param {Array} results - Результаты поиска
   * @param {Object} options - Параметры запроса
   */
  saveResults(query, engine, results, options) {
    try {
      // Формируем имя файла на основе запроса
      const safeQuery = query.replace(/[^\w\-]+/g, '_').toLowerCase();
      const cityInfo = options.loc ? `-loc${options.loc}` : '';
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      
      const fileName = `${engine.toLowerCase()}-${safeQuery}${cityInfo}-${timestamp}.json`;
      const filePath = path.join(this.resultsDir, fileName);
      
      // Структура для сохранения
      const dataToSave = {
        query,
        engine,
        timestamp: new Date().toISOString(),
        options,
        resultsCount: results.length,
        results
      };
      
      // Сохраняем в JSON
      fs.writeJsonSync(filePath, dataToSave, { spaces: 2 });
      
      this.log(`Результаты для "${query}" сохранены в ${fileName}`, 'info');
    } catch (error) {
      this.log(`Ошибка при сохранении результатов для "${query}": ${error.message}`, 'error');
    }
  }
  
  /**
   * Добавляет запрос в очередь с учетом максимального количества потоков
   * @param {Function} requestFunction - Функция запроса
   * @param {Array} requestArgs - Аргументы для функции запроса
   * @returns {Promise} - Промис с результатом выполнения запроса
   */
  enqueueRequest(requestFunction, requestArgs) {
    return new Promise((resolve, reject) => {
      // Добавляем запрос в очередь
      this.requestQueue.push({
        func: requestFunction,
        args: requestArgs,
        resolve,
        reject
      });
      
      // Пытаемся выполнить запрос сразу, если есть свободные слоты
      this.processNextRequest();
    });
  }
  
  /**
   * Обрабатывает следующий запрос из очереди
   */
  processNextRequest() {
    // Если нет запросов в очереди или достигнут лимит активных запросов, выходим
    if (this.requestQueue.length === 0 || this.activeRequests >= this.maxThreads) {
      return;
    }
    
    // Берем следующий запрос из очереди
    const nextRequest = this.requestQueue.shift();
    
    // Выполняем запрос
    try {
      this.activeRequests++;
      
      // Вызываем функцию запроса
      nextRequest.func(...nextRequest.args)
        .then(result => nextRequest.resolve(result))
        .catch(error => nextRequest.reject(error))
        .finally(() => {
          this.activeRequests--;
          // Запускаем обработку следующего запроса
          this.processNextRequest();
        });
    } catch (error) {
      this.activeRequests--;
      nextRequest.reject(error);
      this.processNextRequest();
    }
  }
  
  /**
   * Ожидает освобождения слота для запроса
   * @returns {Promise<void>}
   */
  async waitForAvailableSlot() {
    if (this.activeRequests < this.maxThreads) {
      return;
    }
    
    return new Promise(resolve => {
      const checkInterval = setInterval(() => {
        if (this.activeRequests < this.maxThreads) {
          clearInterval(checkInterval);
          resolve();
        }
      }, 100);
    });
  }
  
  /**
   * Выполняет сбор данных по списку запросов для определенного города
   * @param {Array} queries - Список поисковых запросов
   * @param {Object} options - Параметры запроса
   * @returns {Promise<Array>} - Результаты поиска
   */
  async batchSearchYandex(queries, options = {}) {
    const results = [];
    
    for (const query of queries) {
      try {
        const result = await this.enqueueRequest(
          this.searchYandex.bind(this), 
          [query, options]
        );
        
        results.push(result);
      } catch (error) {
        this.log(`Ошибка при выполнении запроса "${query}": ${error.message}`, 'error');
        
        // Добавляем информацию об ошибке
        results.push({
          query,
          engine: 'YANDEX',
          error: error.message,
          options,
          timestamp: new Date().toISOString()
        });
      }
    }
    
    return results;
  }
  
  /**
   * Выполняет сбор данных по списку запросов для Google
   * @param {Array} queries - Список поисковых запросов
   * @param {Object} options - Параметры запроса
   * @returns {Promise<Array>} - Результаты поиска
   */
  async batchSearchGoogle(queries, options = {}) {
    const results = [];
    
    for (const query of queries) {
      try {
        const result = await this.enqueueRequest(
          this.searchGoogle.bind(this), 
          [query, options]
        );
        
        results.push(result);
      } catch (error) {
        this.log(`Ошибка при выполнении запроса "${query}": ${error.message}`, 'error');
        
        // Добавляем информацию об ошибке
        results.push({
          query,
          engine: 'GOOGLE',
          error: error.message,
          options,
          timestamp: new Date().toISOString()
        });
      }
    }
    
    return results;
  }
  
  /**
   * Выполняет сбор данных по городам-миллионникам для Яндекса
   * @param {Array} queries - Список поисковых запросов
   * @param {Object} options - Дополнительные параметры
   * @returns {Promise<Object>} - Результаты поиска по городам
   */
  async searchYandexByCities(queries, options = {}) {
    const results = {};
    
    // Для каждого города
    for (const [cityName, locId] of Object.entries(YANDEX_CITIES)) {
      this.log(`Начинаем сбор данных для города ${cityName} (loc=${locId})`, 'info');
      
      const cityOptions = {
        ...options,
        loc: locId,
        groupby: options.groupby || 100
      };
      
      // Выполняем поиск для текущего города
      const cityResults = await this.batchSearchYandex(queries, cityOptions);
      
      results[cityName] = {
        locId,
        results: cityResults,
        count: cityResults.length,
        timestamp: new Date().toISOString()
      };
      
      this.log(`Завершен сбор данных для города ${cityName}. Собрано ${cityResults.length} запросов.`, 'info');
    }
    
    return results;
  }
  
  /**
   * Логирование сообщений
   * @param {string} message - Сообщение для логирования
   * @param {string} level - Уровень логирования (info, warn, error)
   */
  log(message, level = 'info') {
    // Если отключен режим отладки и уровень не error, не логируем
    if (!this.debug && level !== 'error') {
      return;
    }
    
    const timestamp = new Date().toISOString();
    let prefix = '';
    
    switch (level) {
      case 'error':
        prefix = '❌ ERROR';
        break;
      case 'warn':
        prefix = '⚠️ WARN';
        break;
      case 'info':
      default:
        prefix = 'ℹ️ INFO';
    }
    
    console.log(`[${timestamp}] ${prefix}: ${message}`);
  }
}

// Экспортируем класс и константы
module.exports = {
  XmlRiverApi,
  YANDEX_CITIES,
  GOOGLE_DOMAINS,
  GOOGLE_COUNTRIES
};
