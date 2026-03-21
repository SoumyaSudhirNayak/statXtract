import { useState } from "react";
import { useNavigate } from "react-router";
import { Header } from "../../components/Header";
import { Navigation } from "../../components/Navigation";
import { BackButton } from "../../components/BackButton";
import { Download } from "lucide-react";
import { BarChart, Bar, PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts";
import { AnimatedBackground } from "../../components/AnimatedBackground";

export function QueryData() {
  const navigate = useNavigate();
  const adminData = JSON.parse(sessionStorage.getItem("adminData") || '{"fullName":"Admin"}');
  const [formData, setFormData] = useState({
    schema: "",
    table: "",
    columns: [] as string[],
    filters: "",
    limit: "100",
    offset: "0",
  });
  const [showResults, setShowResults] = useState(false);
  const [viewMode, setViewMode] = useState<"table" | "bar" | "pie">("table");
  const [hoveredBox, setHoveredBox] = useState<string | null>(null);

  const mockData = [
    { id: 1, state: "Maharashtra", population: 112374333, literacy: 82.34, gdp: 35000 },
    { id: 2, state: "Uttar Pradesh", population: 199812341, literacy: 67.68, gdp: 21000 },
    { id: 3, state: "Karnataka", population: 61095297, literacy: 75.36, gdp: 16000 },
    { id: 4, state: "Tamil Nadu", population: 72147030, literacy: 80.09, gdp: 23000 },
    { id: 5, state: "Gujarat", population: 60439692, literacy: 78.03, gdp: 18000 },
  ];

  const chartData = mockData.map((item) => ({
    name: item.state,
    value: item.population / 1000000,
  }));

  const COLORS = ["#2E5BBA", "#2E7D32", "#F4A300", "#4A90E2", "#81C784"];

  const handleExecute = () => {
    setShowResults(true);
  };

  return (
    <div className="min-h-screen relative">
      <AnimatedBackground />
      
      <div className="relative z-10">
        <Header userName={adminData.fullName} userRole="admin" />
        <Navigation userRole="admin" />

        <div className="max-w-[1800px] mx-auto px-10 py-12">
          <div className="mb-10 flex justify-between items-center">
            <BackButton to="/admin/dashboard" />
            <h1 className="text-4xl text-[#1C1C1C] m-0" style={{ fontFamily: 'var(--font-head)', fontWeight: 800 }}>Query Data</h1>
            <div className="w-[80px]"></div>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-4 gap-12">
            {/* Query Form */}
            <div className="lg:col-span-1">
              <div 
                onMouseEnter={() => setHoveredBox('query-form')}
                onMouseLeave={() => setHoveredBox(null)}
                className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl p-10 space-y-8 transition-all duration-300 shadow-xl"
                style={{
                  borderColor: hoveredBox === 'query-form' ? '#2E5BBA' : '#D1D5DB',
                  boxShadow: hoveredBox === 'query-form' ? '0 15px 40px rgba(46, 91, 186, 0.2)' : 'none'
                }}
              >
                <div>
                  <label className="block text-[#1C1C1C] mb-3 text-lg font-bold">Select Schema</label>
                  <select
                    value={formData.schema}
                    onChange={(e) => setFormData({ ...formData, schema: e.target.value })}
                    onMouseEnter={() => setHoveredBox('schema-select')}
                    onMouseLeave={() => setHoveredBox('query-form')}
                    className="w-full px-5 py-3 border-2 rounded-lg focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                    style={{
                      borderColor: hoveredBox === 'schema-select' ? '#2E5BBA' : '#D1D5DB',
                      boxShadow: hoveredBox === 'schema-select' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                    }}
                  >
                    <option value="">Choose schema...</option>
                    <option value="census">Census Data</option>
                    <option value="economic">Economic</option>
                  </select>
                </div>

                <div>
                  <label className="block text-[#1C1C1C] mb-3 text-lg font-bold">Select Table</label>
                  <select
                    value={formData.table}
                    onChange={(e) => setFormData({ ...formData, table: e.target.value })}
                    onMouseEnter={() => setHoveredBox('table-select')}
                    onMouseLeave={() => setHoveredBox('query-form')}
                    className="w-full px-5 py-3 border-2 rounded-lg focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                    style={{
                      borderColor: hoveredBox === 'table-select' ? '#2E5BBA' : '#D1D5DB',
                      boxShadow: hoveredBox === 'table-select' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                    }}
                  >
                    <option value="">Choose table...</option>
                    <option value="states">State Data</option>
                    <option value="districts">District Data</option>
                  </select>
                </div>

                <div>
                  <label className="block text-[#1C1C1C] mb-3 text-lg font-bold">Select Columns</label>
                  <div className="space-y-4">
                    <div className="flex gap-3">
                      <button 
                        onMouseEnter={() => setHoveredBox('select-all')}
                        onMouseLeave={() => setHoveredBox('query-form')}
                        className="lift-on-hover text-sm px-4 py-2 bg-[#F5F7FA] border-2 rounded-lg cursor-pointer hover:bg-white transition-all duration-300 font-bold"
                        style={{
                          borderColor: hoveredBox === 'select-all' ? '#2E5BBA' : '#D1D5DB',
                          boxShadow: hoveredBox === 'select-all' ? '0 0 8px rgba(46, 91, 186, 0.2)' : 'none'
                        }}
                      >
                        Select All
                      </button>
                      <button 
                        onMouseEnter={() => setHoveredBox('deselect-all')}
                        onMouseLeave={() => setHoveredBox('query-form')}
                        className="lift-on-hover text-sm px-4 py-2 bg-[#F5F7FA] border-2 rounded-lg cursor-pointer hover:bg-white transition-all duration-300 font-bold"
                        style={{
                          borderColor: hoveredBox === 'deselect-all' ? '#2E5BBA' : '#D1D5DB',
                          boxShadow: hoveredBox === 'deselect-all' ? '0 0 8px rgba(46, 91, 186, 0.2)' : 'none'
                        }}
                      >
                        Deselect All
                      </button>
                    </div>
                    <div 
                      onMouseEnter={() => setHoveredBox('columns-box')}
                      onMouseLeave={() => setHoveredBox('query-form')}
                      className="border-2 rounded-xl p-5 max-h-48 overflow-y-auto bg-white transition-all duration-300"
                      style={{
                        borderColor: hoveredBox === 'columns-box' ? '#2E5BBA' : '#D1D5DB',
                        boxShadow: hoveredBox === 'columns-box' ? '0 0 10px rgba(46, 91, 186, 0.2)' : 'none'
                      }}
                    >
                      {["state", "population", "literacy", "gdp"].map((col) => (
                        <label key={col} className="flex items-center gap-3 mb-3 cursor-pointer hover:text-[#2E5BBA] transition-colors">
                          <input type="checkbox" className="w-5 h-5 cursor-pointer accent-[#2E5BBA]" />
                          <span className="text-lg font-medium">{col}</span>
                        </label>
                      ))}
                    </div>
                  </div>
                </div>

                <div>
                  <label className="block text-[#1C1C1C] mb-3 text-lg font-bold">Filters</label>
                  <input
                    type="text"
                    value={formData.filters}
                    onChange={(e) => setFormData({ ...formData, filters: e.target.value })}
                    onMouseEnter={() => setHoveredBox('filters-input')}
                    onMouseLeave={() => setHoveredBox('query-form')}
                    placeholder="e.g., population > 50000000"
                    className="w-full px-5 py-3 border-2 rounded-lg focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                    style={{
                      borderColor: hoveredBox === 'filters-input' ? '#2E5BBA' : '#D1D5DB',
                      boxShadow: hoveredBox === 'filters-input' ? '0 0 10px rgba(46, 91, 186, 0.2)' : 'none'
                    }}
                  />
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-[#1C1C1C] mb-3 text-lg font-bold">Limit</label>
                    <input
                      type="number"
                      value={formData.limit}
                      onChange={(e) => setFormData({ ...formData, limit: e.target.value })}
                      onMouseEnter={() => setHoveredBox('limit-input')}
                      onMouseLeave={() => setHoveredBox('query-form')}
                      className="w-full px-5 py-3 border-2 rounded-lg focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                      style={{
                        borderColor: hoveredBox === 'limit-input' ? '#2E5BBA' : '#D1D5DB',
                        boxShadow: hoveredBox === 'limit-input' ? '0 0 10px rgba(46, 91, 186, 0.2)' : 'none'
                      }}
                    />
                  </div>
                  <div>
                    <label className="block text-[#1C1C1C] mb-3 text-lg font-bold">Offset</label>
                    <input
                      type="number"
                      value={formData.offset}
                      onChange={(e) => setFormData({ ...formData, offset: e.target.value })}
                      onMouseEnter={() => setHoveredBox('offset-input')}
                      onMouseLeave={() => setHoveredBox('query-form')}
                      className="w-full px-5 py-3 border-2 rounded-lg focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                      style={{
                        borderColor: hoveredBox === 'offset-input' ? '#2E5BBA' : '#D1D5DB',
                        boxShadow: hoveredBox === 'offset-input' ? '0 0 10px rgba(46, 91, 186, 0.2)' : 'none'
                      }}
                    />
                  </div>
                </div>

                <button
                  onClick={handleExecute}
                  onMouseEnter={() => setHoveredBox('execute-btn')}
                  onMouseLeave={() => setHoveredBox('query-form')}
                  className="lift-on-hover w-full bg-[#2E5BBA] text-white py-5 rounded-xl border-2 cursor-pointer transition-all duration-300 text-2xl font-black shadow-lg"
                  style={{
                    borderColor: hoveredBox === 'execute-btn' ? '#F4A300' : '#2E5BBA',
                    boxShadow: hoveredBox === 'execute-btn' ? '0 10px 25px rgba(244, 163, 0, 0.4)' : 'none'
                  }}
                >
                  Execute Query
                </button>
              </div>
            </div>

            {/* Results */}
            <div className="lg:col-span-3">
              {showResults ? (
                <div className="space-y-12">
                  {/* Table View - Always show first */}
                  <div 
                    onMouseEnter={() => setHoveredBox('results-table')}
                    onMouseLeave={() => setHoveredBox(null)}
                    className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl p-10 transition-all duration-300 shadow-xl"
                    style={{
                      borderColor: hoveredBox === 'results-table' ? '#2E5BBA' : '#D1D5DB',
                      boxShadow: hoveredBox === 'results-table' ? '0 15px 40px rgba(46, 91, 186, 0.2)' : 'none'
                    }}
                  >
                    <div className="flex justify-between items-center mb-10 pb-6 border-b-2 border-[#E5E7EB]">
                      <h3 className="text-3xl text-[#1C1C1C] m-0 font-bold" style={{ fontFamily: 'var(--font-head)' }}>Query Results</h3>
                      <div className="flex gap-4">
                        <button className="lift-on-hover flex items-center gap-3 px-8 py-3 bg-[#2E7D32] text-white rounded-xl border-2 border-[#2E7D32] hover:border-[#F4A300] cursor-pointer text-xl font-bold transition-all shadow-lg">
                          <Download className="w-6 h-6" />
                          CSV
                        </button>
                        <button className="lift-on-hover flex items-center gap-3 px-8 py-3 bg-[#2E7D32] text-white rounded-xl border-2 border-[#2E7D32] hover:border-[#F4A300] cursor-pointer text-xl font-bold transition-all shadow-lg">
                          JSON
                        </button>
                      </div>
                    </div>

                    <div className="overflow-x-auto border-2 border-[#D1D5DB] rounded-xl bg-white">
                      <table className="w-full text-xl">
                        <thead>
                          <tr className="bg-[#1F3A5F] text-white">
                            <th className="px-8 py-6 text-left font-black uppercase tracking-wider">State</th>
                            <th className="px-8 py-6 text-left font-black uppercase tracking-wider">Population</th>
                            <th className="px-8 py-6 text-left font-black uppercase tracking-wider">Literacy %</th>
                            <th className="px-8 py-6 text-left font-black uppercase tracking-wider">GDP</th>
                          </tr>
                        </thead>
                        <tbody>
                          {mockData.map((row) => (
                            <tr key={row.id} className="border-b border-[#E5E7EB] hover:bg-[#2E5BBA]/5 transition-colors">
                              <td className="px-8 py-6 text-[#1C1C1C] font-bold">{row.state}</td>
                              <td className="px-8 py-6 text-[#1C1C1C] font-medium">{row.population.toLocaleString()}</td>
                              <td className="px-8 py-6 text-[#1C1C1C] font-medium">{row.literacy}%</td>
                              <td className="px-8 py-6 text-[#1C1C1C] font-medium">₹{row.gdp.toLocaleString()}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>

                  {/* Charts Section - Below table */}
                  <div 
                    onMouseEnter={() => setHoveredBox('results-charts')}
                    onMouseLeave={() => setHoveredBox(null)}
                    className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl p-10 transition-all duration-300 shadow-xl"
                    style={{
                      borderColor: hoveredBox === 'results-charts' ? '#2E7D32' : '#D1D5DB',
                      boxShadow: hoveredBox === 'results-charts' ? '0 15px 40px rgba(46, 125, 50, 0.2)' : 'none'
                    }}
                  >
                    <h3 className="text-3xl text-[#1C1C1C] mb-10 font-bold" style={{ fontFamily: 'var(--font-head)' }}>Data Visualization</h3>
                    
                    <div className="flex gap-4 mb-10">
                      <button
                        onClick={() => setViewMode("bar")}
                        className={`lift-on-hover px-8 py-3 rounded-xl border-2 cursor-pointer text-xl font-bold transition-all shadow-lg ${
                          viewMode === "bar" ? "bg-[#2E5BBA] text-white border-[#2E5BBA]" : "bg-white text-[#1C1C1C] border-[#D1D5DB] hover:border-[#2E5BBA]"
                        }`}
                      >
                        Bar Chart
                      </button>
                      <button
                        onClick={() => setViewMode("pie")}
                        className={`lift-on-hover px-8 py-3 rounded-xl border-2 cursor-pointer text-xl font-bold transition-all shadow-lg ${
                          viewMode === "pie" ? "bg-[#2E5BBA] text-white border-[#2E5BBA]" : "bg-white text-[#1C1C1C] border-[#D1D5DB] hover:border-[#2E5BBA]"
                        }`}
                      >
                        Pie Chart
                      </button>
                    </div>

                    <div className="p-8 border-2 border-[#D1D5DB] rounded-xl bg-white h-[600px]">
                      {viewMode === "bar" && (
                        <ResponsiveContainer width="100%" height="100%">
                          <BarChart data={chartData}>
                            <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#E5E7EB" />
                            <XAxis dataKey="name" tick={{ fontSize: 14, fontWeight: 'bold' }} />
                            <YAxis tick={{ fontSize: 14, fontWeight: 'bold' }} />
                            <Tooltip contentStyle={{ fontSize: '16px', borderRadius: '10px', border: '2px solid #D1D5DB' }} />
                            <Legend wrapperStyle={{ paddingTop: '20px', fontSize: '16px', fontWeight: 'bold' }} />
                            <Bar dataKey="value" fill="#2E5BBA" radius={[8, 8, 0, 0]} name="Population (millions)" />
                          </BarChart>
                        </ResponsiveContainer>
                      )}

                      {viewMode === "pie" && (
                        <ResponsiveContainer width="100%" height="100%">
                          <PieChart>
                            <Pie
                              data={chartData}
                              cx="50%"
                              cy="50%"
                              labelLine={false}
                              label={(entry) => entry.name}
                              outerRadius={200}
                              fill="#8884d8"
                              dataKey="value"
                            >
                              {chartData.map((entry, index) => (
                                <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                              ))}
                            </Pie>
                            <Tooltip contentStyle={{ fontSize: '16px', borderRadius: '10px', border: '2px solid #D1D5DB' }} />
                            <Legend wrapperStyle={{ paddingTop: '20px', fontSize: '16px', fontWeight: 'bold' }} />
                          </PieChart>
                        </ResponsiveContainer>
                      )}
                    </div>
                  </div>
                </div>
              ) : (
                <div 
                  onMouseEnter={() => setHoveredBox('no-results')}
                  onMouseLeave={() => setHoveredBox(null)}
                  className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 border-dashed rounded-xl p-32 text-center transition-all duration-300"
                  style={{
                    borderColor: hoveredBox === 'no-results' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredBox === 'no-results' ? '0 20px 50px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                >
                  <p className="text-[#6B7280] text-3xl font-bold leading-relaxed">Select schema, table and columns then click Execute to view data</p>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}