﻿using System;
using System.Collections.Generic;
using System.Text;

namespace CosmosDB
{
    class Course
    {
        public string id { get; set; }
        public string coursename { get; set; }
        public string customerid { get; set; }
        public decimal rating { get; set; }
        public List<Order> orders { get; set; }
    }
}
