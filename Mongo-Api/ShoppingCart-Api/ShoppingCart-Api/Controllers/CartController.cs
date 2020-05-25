using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using MongoDB.Bson;
using MongoDB.Driver;


namespace ShoppingCart_Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CartController : ControllerBase
    {
        MongoClient dbClient = new MongoClient("mongodb+srv://admin:Skole123@cluster0-wrdn4.mongodb.net/test");
        
        // GET: api/Cart
        [HttpGet]
        public IEnumerable<string> Get()
        {
            return new string[] { "value1", "value2" };
        }

        // GET: api/Cart/5
        [HttpGet("{id}", Name = "Get")]
        public string Get(int id)
        {
            return "value";
        }

        // POST: api/Cart
        [HttpPost]
        public void Post([FromBody] string value)
        {
            var database = dbClient.GetDatabase("Exam");
            var collection = database.GetCollection<BsonDocument>("Cart");

            var document = BsonDocument.Parse(value);

            collection.InsertOne(document);
        }

        // PUT: api/Cart/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody] string value)
        {
        }

        // DELETE: api/ApiWithActions/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }
    }
}
