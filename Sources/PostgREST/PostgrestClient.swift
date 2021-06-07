//
//  File.swift
//
//
//  Created by Satish.Babariya on 04/06/21.
//

import Foundation

public struct PostgrestError: Error {
    public var details: String?
    public var hint: String?
    public var code: String?
    public var message: String

    init?(from dictionary: [String: Any]) {
        guard let details = dictionary["details"] as? String,
              let hint = dictionary["hint"] as? String,
              let code = dictionary["code"] as? String,
              let message = dictionary["message"] as? String
        else {
            return nil
        }
        self.details = details
        self.hint = hint
        self.code = code
        self.message = message
    }

    init(message: String) {
        self.message = message
    }
}

extension PostgrestError: LocalizedError {
    public var errorDescription: String? {
        return message
    }
}

class PostgrestResponse {
    var body: Any
    var status: Int?
    var count: Int?
    var error: PostgrestError?

    init(body: Any) {
        self.body = body
    }

    init?(from dictionary: [String: Any]) {
        guard let body = dictionary["body"] else {
            return nil
        }
        self.body = body

        if let status: Int = dictionary["status"] as? Int {
            self.status = status
        }

        if let count: Int = dictionary["count"] as? Int {
            self.count = count
        }

        if let error: [String: Any] = dictionary["error"] as? [String: Any] {
            self.error = PostgrestError(from: error)
        }
    }
}

enum CountOption: String {
    case exact
    case planned
    case estimated
}

class PostgrestBuilder {
    var url: String
    var headers: [String: String]
    var schema: String?
    var method: String?
    var body: [String: Any]?

    public init(url: String, headers: [String: String] = [:], schema: String?) {
        self.url = url
        self.headers = headers
        self.schema = schema
    }

    public init(url: String, method: String?, headers: [String: String] = [:], schema: String?, body: [String: Any]?) {
        self.url = url
        self.headers = headers
        self.schema = schema
        self.method = method
        self.body = body
    }

    public func execute(head: Bool = false, count: CountOption? = nil, completion: @escaping (Result<PostgrestResponse, Error>) -> Void) {
        if head {
            method = "HEAD"
        }

        if let count = count {
            if let prefer = headers["Prefer"] {
                headers["Prefer"] = "\(prefer),count=\(count.rawValue)"
            } else {
                headers["Prefer"] = "count=\(count.rawValue)"
            }
        }

        if method == nil {
            completion(.failure(PostgrestError(message: "Missing table operation: select, insert, update or delete")))
            return
        }

        if let method = method, method == "GET" || method == "HEAD" {
            headers["Content-Type"] = "application/json"
        }

        if let schema = schema {
            if let method = method, method == "GET" || method == "HEAD" {
                headers["Accept-Profile"] = schema
            } else {
                headers["Content-Profile"] = schema
            }
        }

        guard let url = URL(string: url) else {
            completion(.failure(PostgrestError(message: "badURL")))
            return
        }

        var request = URLRequest(url: url)
        request.httpMethod = method
        request.allHTTPHeaderFields = headers

        let session = URLSession.shared
        let dataTask = session.dataTask(with: request, completionHandler: { [unowned self] (data, response, error) -> Void in
            if let error = error {
                completion(.failure(error))
                return
            }

            if let resp = response as? HTTPURLResponse {
                if let data = data {
                    do {
                        completion(.success(try self.parse(data: data, response: resp)))
                    } catch {
                        completion(.failure(error))
                        return
                    }
                }
            } else {
                completion(.failure(PostgrestError(message: "failed to get response")))
            }

        })

        dataTask.resume()
    }

    private func parse(data: Data, response: HTTPURLResponse) throws -> PostgrestResponse {
        if response.statusCode == 200 || 200 ..< 300 ~= response.statusCode {
            var body: Any = data
            var count: Int?

            if let method = method, method == "HEAD" {
                if let accept = response.allHeaderFields["Accept"] as? String, accept == "text/csv" {
                    body = data
                } else {
                    do {
                        let json = try JSONSerialization.jsonObject(with: data, options: [])
                        body = json
                    } catch {
                        throw error
                    }
                }
            }

            if let contentRange = response.allHeaderFields["content-range"] as? String, let lastElement = contentRange.split(separator: "/").last {
                count = lastElement == "*" ? nil : Int(lastElement)
            }

            let postgrestResponse = PostgrestResponse(body: body)
            postgrestResponse.status = response.statusCode
            postgrestResponse.count = count
            return postgrestResponse
        } else {
            do {
                let json = try JSONSerialization.jsonObject(with: data, options: [])
                if let errorJson: [String: Any] = json as? [String: Any] {
                    throw PostgrestError(from: errorJson) ?? PostgrestError(message: "failed to get error")
                } else {
                    throw PostgrestError(message: "failed to get error")
                }
            } catch {
                throw error
            }
        }
    }
    
    func appendSearchParams(name: String, value: String) {
        var urlComponent = URLComponents.init(string: url)
        urlComponent?.queryItems?.append(URLQueryItem.init(name: name, value: value))
        self.url = urlComponent?.url?.absoluteString ?? self.url
    }
}

// class PostgrestQueryBuilder: PostgrestBuilder {
//
//    public override init(url: String, headers: [String: String] = [:], schema: String?){
//            super.init(url: url, headers: headers, schema: schema)
//        }
//
//    func select(<#parameters#>) -> <#return type#> {
//        <#function body#>
//    }
// }

//
class PostgrestTransformBuilder: PostgrestBuilder {
    override init(url: String, method: String?, headers: [String: String] = [:], schema: String?, body: [String: Any]?) {
        super.init(url: url, method: method, headers: headers, schema: schema, body: body)
    }
    
    func select(columns: String = "*") -> PostgrestTransformBuilder {
        method = "GET"
        var quoted = false
        let cleanedColumns = columns.compactMap { (char) -> String? in
            if char.isWhitespace && !quoted {
                return nil
            }
            if char == "\"" {
                quoted = !quoted
            }
            return String(char)
        }.reduce("", +)
        appendSearchParams(name: "select", value: cleanedColumns)
        return PostgrestTransformBuilder(url: url, method: method, headers: headers, schema: schema, body: body)
    }
    
    func insert(values:[String: Any], upsert: Bool = false, onConflict: String? = nil) -> PostgrestBuilder {
        method = "POST"
        headers["Prefer"] = upsert ? "return=representation,resolution=merge-duplicates" : "return=representation"
        if let onConflict = onConflict {
            appendSearchParams(name: "on_conflict", value: onConflict)
        }
        
        body = values
        return self
    }
    
    func upsert(values:[String: Any], onConflict: String? = nil) -> PostgrestBuilder {
        method = "POST"
        headers["Prefer"] = "return=representation,resolution=merge-duplicates"
        if let onConflict = onConflict {
            appendSearchParams(name: "on_conflict", value: onConflict)
        }
        
        body = values
        return self
    }
    
    func update(values:[String: Any]) -> PostgrestFilterBuilder {
        method = "PATCH"
        headers["Prefer"] = "return=representation"
        body = values
        return PostgrestFilterBuilder.init(url: url, method: method, headers: headers, schema: schema, body: body)
    }
    
    func delete() -> PostgrestFilterBuilder {
        method = "DELETE"
        headers["Prefer"] = "return=representation"
        return PostgrestFilterBuilder.init(url: url, method: method, headers: headers, schema: schema, body: body)
    }
}

//
 class PostgrestFilterBuilder: PostgrestTransformBuilder {

    enum Operator: String {
        case eq, neq, gt, gte, lt, lte, like, ilike, `is`, `in`, cs, cd, sl, sr, nxl, nxr, adj, ov, fts, plfts, phfts, wfts
    }
    
    func not(column: String, operator op: Operator, value:String) -> PostgrestFilterBuilder {
        appendSearchParams(name: column, value: "not.\(op.rawValue).\(value)")
        return PostgrestFilterBuilder.init(url: url, method: method, headers: headers, schema: schema, body: body)
    }
    
    func or(filters: String) -> PostgrestFilterBuilder {
        appendSearchParams(name: "or", value: "(\(filters))")
        return PostgrestFilterBuilder.init(url: url, method: method, headers: headers, schema: schema, body: body)
    }
    
    func eq(column: String, value:String) -> PostgrestFilterBuilder {
        appendSearchParams(name: column, value: "eq.\(value)")
        return PostgrestFilterBuilder.init(url: url, method: method, headers: headers, schema: schema, body: body)
    }
    
    func neq(column: String, value:String) -> PostgrestFilterBuilder {
        appendSearchParams(name: column, value: "neq.\(value)")
        return PostgrestFilterBuilder.init(url: url, method: method, headers: headers, schema: schema, body: body)
    }

    func gt(column: String, value:String) -> PostgrestFilterBuilder {
        appendSearchParams(name: column, value: "gt.\(value)")
        return PostgrestFilterBuilder.init(url: url, method: method, headers: headers, schema: schema, body: body)
    }
    
    func gte(column: String, value:String) -> PostgrestFilterBuilder {
        appendSearchParams(name: column, value: "gte.\(value)")
        return PostgrestFilterBuilder.init(url: url, method: method, headers: headers, schema: schema, body: body)
    }
    
    func lt(column: String, value:String) -> PostgrestFilterBuilder {
        appendSearchParams(name: column, value: "lt.\(value)")
        return PostgrestFilterBuilder.init(url: url, method: method, headers: headers, schema: schema, body: body)
    }
    
    func lte(column: String, value:String) -> PostgrestFilterBuilder {
        appendSearchParams(name: column, value: "lte.\(value)")
        return PostgrestFilterBuilder.init(url: url, method: method, headers: headers, schema: schema, body: body)
    }
    
    func like(column: String, value:String) -> PostgrestFilterBuilder {
        appendSearchParams(name: column, value: "like.\(value)")
        return PostgrestFilterBuilder.init(url: url, method: method, headers: headers, schema: schema, body: body)
    }
    
    func ilike(column: String, value:String) -> PostgrestFilterBuilder {
        appendSearchParams(name: column, value: "ilike.\(value)")
        return PostgrestFilterBuilder.init(url: url, method: method, headers: headers, schema: schema, body: body)
    }
    
    func `is`(column: String, value:String) -> PostgrestFilterBuilder {
        appendSearchParams(name: column, value: "is.\(value)")
        return PostgrestFilterBuilder.init(url: url, method: method, headers: headers, schema: schema, body: body)
    }
    
    func `in`(column: String, value:[String]) -> PostgrestFilterBuilder {
        appendSearchParams(name: column, value: "in.\(value.joined(separator: ","))")
        return PostgrestFilterBuilder.init(url: url, method: method, headers: headers, schema: schema, body: body)
    }
    
    func contains(column: String, value:Any) -> PostgrestFilterBuilder {
        if let str: String = value as? String {
            appendSearchParams(name: column, value: "cs.\(str)")
        }else if let arr: [String] = value as? [String] {
            appendSearchParams(name: column, value: "cs.\(arr.joined(separator: ","))")
        }else if let data: Data = try? JSONSerialization.data(withJSONObject: value, options: []), let json = String.init(data: data, encoding: .utf8)  {
            appendSearchParams(name: column, value: "cs.\(json)")
        }
        return PostgrestFilterBuilder.init(url: url, method: method, headers: headers, schema: schema, body: body)
    }
    
    
    func rangeLt(column: String, range:String) -> PostgrestFilterBuilder {
        appendSearchParams(name: column, value: "sl.\(range)")
        return PostgrestFilterBuilder.init(url: url, method: method, headers: headers, schema: schema, body: body)
    }
    
    func rangeGt(column: String, range:String) -> PostgrestFilterBuilder {
        appendSearchParams(name: column, value: "sr.\(range)")
        return PostgrestFilterBuilder.init(url: url, method: method, headers: headers, schema: schema, body: body)
    }
    
    func rangeGte(column: String, range:String) -> PostgrestFilterBuilder {
        appendSearchParams(name: column, value: "nxl.\(range)")
        return PostgrestFilterBuilder.init(url: url, method: method, headers: headers, schema: schema, body: body)
    }
    
    func rangeLte(column: String, range:String) -> PostgrestFilterBuilder {
        appendSearchParams(name: column, value: "nxr.\(range)")
        return PostgrestFilterBuilder.init(url: url, method: method, headers: headers, schema: schema, body: body)
    }
    
    func rangeAdjacent(column: String, range:String) -> PostgrestFilterBuilder {
        appendSearchParams(name: column, value: "adj.\(range)")
        return PostgrestFilterBuilder.init(url: url, method: method, headers: headers, schema: schema, body: body)
    }

    
 }
//
// class PostgrestQueryBuilder: PostgrestBuilder {
//    public override init(url: String, headers: [String: String] = [:], schema: String?){
//        super.init(url: url, headers: headers, schema: schema)
//    }
//
//    enum Count: String {
//        case exact = "exact"
//        case planned = "planned"
//        case estimated = "estimated"
//    }
//
//    func select(_ columns: String = "*", head: Bool? = false, count: Count? = nil) -> PostgrestFilterBuilder {
//        self.method = .get
//
//        return PostgrestFilterBuilder(url: url, schema: schema)
//    }
// }
//
// class PostgrestClient {
//    var url: String
//    var headers: [String: String]
//    var schema: String?
//
//    public init(url: String, headers: [String: String] = [:], schema: String?){
//        self.url = url
//        self.headers = headers
//        self.schema = schema
//    }
//
//    public func form(_ table: String) -> PostgrestQueryBuilder {
//        return PostgrestQueryBuilder.init(url: "\(url)/\(table)", headers: headers, schema: schema)
//    }
//
//
// }
