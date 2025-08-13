import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing'
import { TestBed } from '@angular/core/testing'
import IDbConfig from 'src/app/model/db-config'

import { FetchService } from './fetch.service'
import { provideHttpClient } from '@angular/common/http'

describe('FetchService', () => {
  let service: FetchService
  let httpMock: HttpTestingController

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [provideHttpClient(), provideHttpClientTesting()],
    })
    service = TestBed.inject(FetchService)
    httpMock = TestBed.inject(HttpTestingController)
  })

  afterEach(() => {
    httpMock.verify()
  })

  it('should be created', () => {
    expect(service).toBeTruthy()
  })

  describe('connectTodb', () => {
    it('should send a POST request to /connect with the correct payload', () => {
      const mockDbConfig: IDbConfig = {
        dbEngine: 'cassandra',
        isSharded: false,
        hostName: 'localhost',
        port: '9042',
        userName: 'user',
        password: 'password',
        sslMode: false,
        dbName: 'testdb',
        dataCenter: 'dc1',
      }
      const mockDialect = 'google_standard_sql'

      service.connectTodb(mockDbConfig, mockDialect).subscribe()

      const req = httpMock.expectOne('http://localhost:9876/connect')
      expect(req.request.method).toBe('POST')
      expect(req.request.body).toEqual({
        Driver: 'cassandra',
        IsSharded: false,
        Host: 'localhost',
        Port: '9042',
        Database: 'testdb',
        User: 'user',
        Password: 'password',
        Sslmode: false,
        Dialect: 'google_standard_sql',
        DataCenter: 'dc1',
      })
      req.flush(null, { status: 200, statusText: 'OK' })
    })
  })

  describe('setSourceDBDetailsForDirectConnect', () => {
    it('should send a POST request to /SetSourceDBDetailsForDirectConnect with the correct payload', () => {
      const mockDbConfig: IDbConfig = {
        dbEngine: 'cassandra',
        isSharded: false,
        hostName: 'localhost',
        port: '9042',
        userName: 'user',
        password: 'password',
        sslMode: false,
        dbName: 'testdb',
        dataCenter: 'dc1',
      }

      service.setSourceDBDetailsForDirectConnect(mockDbConfig).subscribe()

      const req = httpMock.expectOne('http://localhost:9876/SetSourceDBDetailsForDirectConnect')
      expect(req.request.method).toBe('POST')
      expect(req.request.body).toEqual({
        Driver: 'cassandra',
        Host: 'localhost',
        Port: '9042',
        Database: 'testdb',
        User: 'user',
        Password: 'password',
        Sslmode: false,
        DataCenter: 'dc1',
      })
      req.flush({})
    })
  })
})
