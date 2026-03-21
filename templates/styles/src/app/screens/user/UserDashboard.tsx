import { useState } from "react";
import { useNavigate } from "react-router";
import { LayoutDashboard, Search, Settings, CreditCard, History, Download, User, ChevronLeft, ChevronRight, Menu, Shield, Zap, Crown, Check } from "lucide-react";
import { BarChart, Bar, PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts";
import { AnimatedBackground } from "../../components/AnimatedBackground";

type ViewType = "dashboard" | "query" | "settings" | "plans" | "history" | "downloads";

export function UserDashboard() {
  const navigate = useNavigate();
  const userData = JSON.parse(sessionStorage.getItem("userData") || '{"fullName":"User"}');
  const [activeView, setActiveView] = useState<ViewType>("dashboard");
  const [isSidebarCollapsed, setIsSidebarCollapsed] = useState(false);
  const [hoveredMenuItem, setHoveredMenuItem] = useState<number | null>(null);
  const [hoveredBox, setHoveredBox] = useState<string | null>(null);
  const [queryFormData, setQueryFormData] = useState({
    schema: "",
    table: "",
    columns: [] as string[],
    filters: "",
    limit: "100",
    offset: "0",
  });
  const [showQueryResults, setShowQueryResults] = useState(false);
  const [chartView, setChartView] = useState<"table" | "bar" | "pie">("table");

  const mockData = [
    { id: 1, state: "Maharashtra", population: 112374333, literacy: 82.34 },
    { id: 2, state: "Uttar Pradesh", population: 199812341, literacy: 67.68 },
    { id: 3, state: "Karnataka", population: 61095297, literacy: 75.36 },
    { id: 4, state: "Tamil Nadu", population: 72147030, literacy: 80.09 },
  ];

  const chartData = mockData.map((item) => ({
    name: item.state,
    value: item.population / 1000000,
  }));

  const COLORS = ["#2E5BBA", "#2E7D32", "#F4A300", "#4A90E2"];

  const menuItems = [
    { id: "dashboard" as ViewType, label: "Dashboard", icon: LayoutDashboard, color: "#2E5BBA" },
    { id: "query" as ViewType, label: "Query Page", icon: Search, color: "#2E7D32" },
    { id: "settings" as ViewType, label: "Settings", icon: Settings, color: "#F4A300" },
    { id: "plans" as ViewType, label: "Plans & Pricing", icon: CreditCard, color: "#4A90E2" },
    { id: "history" as ViewType, label: "History", icon: History, color: "#D32F2F" },
    { id: "downloads" as ViewType, label: "Downloads", icon: Download, color: "#7B1FA2" },
  ];

  const handleExecuteQuery = () => {
    setShowQueryResults(true);
  };

  return (
    <div className="h-screen flex flex-col relative">
      <AnimatedBackground />
      
      {/* Header */}
      <div className="relative z-20 bg-[#1F3A5F] text-white px-10 py-6 flex justify-between items-center shadow-xl border-b-2 border-[#2E5BBA]">
        <h1 
          className="text-4xl m-0 font-black tracking-tighter cursor-pointer hover:opacity-80 transition-opacity" 
          onClick={() => navigate("/user/dashboard")}
          style={{ fontFamily: 'var(--font-head)', color: 'white' }}
        >
          STATXTRACT
        </h1>
        <button
          onClick={() => navigate("/user/profile")}
          className="lift-on-hover flex items-center gap-4 bg-[#2E7D32] border-2 border-[#2E7D32] text-white cursor-pointer px-6 py-3 rounded-xl transition-all duration-200 hover:border-[#F4A300] shadow-lg"
          style={{
            boxShadow: '0 4px 15px rgba(0,0,0,0.2)'
          }}
        >
          <div className="bg-white/20 p-2 rounded-full shadow-inner">
            <User className="w-6 h-6 text-white" />
          </div>
          <span className="font-bold text-xl">{userData.fullName || "User"}</span>
        </button>
      </div>

      {/* Main Content */}
      <div className="flex-1 overflow-hidden flex relative z-10">
        {/* Collapsible Sidebar */}
        <div 
          className={`bg-white/80 backdrop-blur-sm border-r-2 border-[#E5E7EB] transition-all duration-300 relative ${
            isSidebarCollapsed ? "w-24" : "w-80"
          }`}
        >
          {/* Toggle Button - Positioned in corner */}
          <button
            onClick={() => setIsSidebarCollapsed(!isSidebarCollapsed)}
            onMouseEnter={() => setHoveredBox('toggle')}
            onMouseLeave={() => setHoveredBox(null)}
            className="absolute -right-4 top-6 z-30 flex items-center justify-center w-8 h-8 bg-[#2E5BBA] text-white rounded-full border-2 transition-all duration-300 cursor-pointer shadow-md"
            style={{
              borderColor: hoveredBox === 'toggle' ? '#F4A300' : '#2E5BBA',
              boxShadow: hoveredBox === 'toggle' ? '0 0 15px rgba(244, 163, 0, 0.6)' : '0 2px 4px rgba(0,0,0,0.1)'
            }}
          >
            {isSidebarCollapsed ? <ChevronRight className="w-5 h-5" /> : <ChevronLeft className="w-5 h-5" />}
          </button>

          <div className="p-6 flex flex-col h-full">
            {/* Navigation */}
            <nav className="space-y-4 mt-12 flex-1">
              {menuItems.map((item, index) => (
                <button
                  key={item.id}
                  onClick={() => setActiveView(item.id)}
                  className={`lift-on-hover w-full flex items-center gap-4 rounded-xl transition-all duration-300 border-2 cursor-pointer text-left ${
                    isSidebarCollapsed ? "justify-center px-0 py-5" : "px-6 py-5"
                  } ${
                    activeView === item.id
                      ? `text-white shadow-lg`
                      : `bg-transparent text-[#1C1C1C] border-transparent hover:bg-white/50`
                  }`}
                  style={{
                    backgroundColor: activeView === item.id ? item.color : "transparent",
                    borderColor: activeView === item.id ? item.color : (hoveredMenuItem === index ? item.color : "transparent"),
                    boxShadow: activeView === item.id ? `0 10px 25px ${item.color}44` : 'none'
                  }}
                  title={isSidebarCollapsed ? item.label : ""}
                  onMouseEnter={() => setHoveredMenuItem(index)}
                  onMouseLeave={() => setHoveredMenuItem(null)}
                >
                  {isSidebarCollapsed ? (
                    <item.icon className="w-10 h-10" style={{ color: activeView === item.id ? "white" : item.color }} />
                  ) : (
                    <>
                      <item.icon className="w-7 h-7" style={{ color: activeView === item.id ? "white" : item.color }} />
                      <span className="text-xl font-bold tracking-tight">{item.label}</span>
                    </>
                  )}
                </button>
              ))}
            </nav>
          </div>
        </div>

        {/* Content Area */}
        <div className="flex-1 overflow-y-auto p-12">
          {/* Dashboard View */}
          {activeView === "dashboard" && (
            <div className="max-w-[1600px] mx-auto">
              <h1 className="text-4xl text-[#1C1C1C] mb-10" style={{ fontFamily: 'var(--font-head)', fontWeight: 800 }}>
                Welcome to STATXTRACT
              </h1>
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-10 mb-10">
                <div 
                  onMouseEnter={() => setHoveredBox('stats')}
                  onMouseLeave={() => setHoveredBox(null)}
                  className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl p-10 transition-all duration-300"
                  style={{
                    borderColor: hoveredBox === 'stats' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredBox === 'stats' ? '0 15px 40px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                >
                  <h3 className="text-2xl text-[#2E5BBA] mb-4" style={{ fontFamily: 'var(--font-head)', fontWeight: 700 }}>Quick Stats</h3>
                  <p className="text-5xl font-bold text-[#1C1C1C] mb-2">150+ Datasets</p>
                  <p className="text-lg text-[#6B7280]">Available for query</p>
                </div>
                <div 
                  onMouseEnter={() => setHoveredBox('activity')}
                  onMouseLeave={() => setHoveredBox(null)}
                  className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl p-10 transition-all duration-300"
                  style={{
                    borderColor: hoveredBox === 'activity' ? '#2E7D32' : '#D1D5DB',
                    boxShadow: hoveredBox === 'activity' ? '0 15px 40px rgba(46, 125, 50, 0.2)' : 'none'
                  }}
                >
                  <h3 className="text-2xl text-[#2E7D32] mb-4" style={{ fontFamily: 'var(--font-head)', fontWeight: 700 }}>Your Activity</h3>
                  <p className="text-5xl font-bold text-[#1C1C1C] mb-2">24 Queries</p>
                  <p className="text-lg text-[#6B7280]">This month</p>
                </div>
              </div>
              <div 
                onMouseEnter={() => setHoveredBox('started')}
                onMouseLeave={() => setHoveredBox(null)}
                className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl p-10 transition-all duration-300"
                style={{
                  borderColor: hoveredBox === 'started' ? '#F4A300' : '#D1D5DB',
                  boxShadow: hoveredBox === 'started' ? '0 15px 40px rgba(244, 163, 0, 0.2)' : 'none'
                }}
              >
                <h3 className="text-3xl text-[#1C1C1C] mb-6" style={{ fontFamily: 'var(--font-head)', fontWeight: 700 }}>
                  Getting Started
                </h3>
                <ul className="text-[#6B7280] text-xl space-y-4 m-0 pl-8 list-disc">
                  <li>Use the Query Page to access and analyze data</li>
                  <li>Download results in multiple formats (CSV, PDF, JSON)</li>
                  <li>View your query history and previous downloads</li>
                  <li>Upgrade your plan for enhanced features</li>
                </ul>
              </div>
            </div>
          )}

          {/* Query Page */}
          {activeView === "query" && (
            <div className="max-w-[1700px] mx-auto">
              <h1 className="text-4xl text-[#1C1C1C] mb-10" style={{ fontFamily: 'var(--font-head)', fontWeight: 800 }}>Query Data</h1>
              
              <div className="grid grid-cols-1 lg:grid-cols-4 gap-10">
                {/* Query Form */}
                <div className="lg:col-span-1">
                  <div 
                    onMouseEnter={() => setHoveredBox('query-form')}
                    onMouseLeave={() => setHoveredBox(null)}
                    className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl p-8 transition-all duration-300 shadow-lg"
                    style={{
                      borderColor: hoveredBox === 'query-form' ? '#2E7D32' : '#D1D5DB',
                      boxShadow: hoveredBox === 'query-form' ? '0 15px 40px rgba(46, 125, 50, 0.2)' : 'none'
                    }}
                  >
                    <h2 className="text-2xl text-[#1C1C1C] mb-8" style={{ fontFamily: 'var(--font-head)', fontWeight: 700 }}>
                      Selection
                    </h2>
                    <div className="space-y-8">
                      <div>
                        <label className="block text-[#1C1C1C] mb-3 text-lg font-medium">Select Schema</label>
                        <select
                          value={queryFormData.schema}
                          onChange={(e) => setQueryFormData({ ...queryFormData, schema: e.target.value })}
                          onMouseEnter={() => setHoveredBox('schema-select')}
                          onMouseLeave={() => setHoveredBox(null)}
                          className="w-full px-4 py-3 border-2 rounded-lg focus:outline-none focus:border-[#F4A300] text-lg bg-white transition-all duration-300"
                          style={{
                            borderColor: hoveredBox === 'schema-select' ? '#2E7D32' : '#D1D5DB',
                            boxShadow: hoveredBox === 'schema-select' ? '0 0 15px rgba(46, 125, 50, 0.3)' : 'none'
                          }}
                        >
                          <option value="">Choose...</option>
                          <option value="census">Census</option>
                        </select>
                      </div>

                      <div>
                        <label className="block text-[#1C1C1C] mb-3 text-lg font-medium">Select Table</label>
                        <select
                          value={queryFormData.table}
                          onChange={(e) => setQueryFormData({ ...queryFormData, table: e.target.value })}
                          onMouseEnter={() => setHoveredBox('table-select')}
                          onMouseLeave={() => setHoveredBox(null)}
                          className="w-full px-4 py-3 border-2 rounded-lg focus:outline-none focus:border-[#F4A300] text-lg bg-white transition-all duration-300"
                          style={{
                            borderColor: hoveredBox === 'table-select' ? '#2E7D32' : '#D1D5DB',
                            boxShadow: hoveredBox === 'table-select' ? '0 0 15px rgba(46, 125, 50, 0.3)' : 'none'
                          }}
                        >
                          <option value="">Choose...</option>
                          <option value="states">States</option>
                        </select>
                      </div>

                      <div>
                        <label className="block text-[#1C1C1C] mb-3 text-lg font-medium">Attributes</label>
                        <div 
                          onMouseEnter={() => setHoveredBox('attributes-box')}
                          onMouseLeave={() => setHoveredBox(null)}
                          className="border-2 rounded-lg p-4 max-h-48 overflow-y-auto bg-white/50 transition-all duration-300"
                          style={{
                            borderColor: hoveredBox === 'attributes-box' ? '#2E7D32' : '#D1D5DB',
                            boxShadow: hoveredBox === 'attributes-box' ? '0 0 15px rgba(46, 125, 50, 0.3)' : 'none'
                          }}
                        >
                          {["state", "population", "literacy"].map((col) => (
                            <label key={col} className="flex items-center gap-3 mb-3 cursor-pointer hover:text-[#2E7D32] transition-colors group">
                              <input type="checkbox" className="w-5 h-5 cursor-pointer accent-[#2E7D32]" />
                              <span className="text-lg group-hover:font-medium">{col}</span>
                            </label>
                          ))}
                        </div>
                      </div>

                      <div>
                        <label className="block text-[#1C1C1C] mb-3 text-lg font-medium">Filters</label>
                        <input
                          type="text"
                          value={queryFormData.filters}
                          onChange={(e) => setQueryFormData({ ...queryFormData, filters: e.target.value })}
                          onMouseEnter={() => setHoveredBox('filters-input')}
                          onMouseLeave={() => setHoveredBox(null)}
                          placeholder="Optional filter criteria"
                          className="w-full px-4 py-3 border-2 rounded-lg focus:outline-none focus:border-[#F4A300] text-lg bg-white transition-all duration-300"
                          style={{
                            borderColor: hoveredBox === 'filters-input' ? '#2E7D32' : '#D1D5DB',
                            boxShadow: hoveredBox === 'filters-input' ? '0 0 15px rgba(46, 125, 50, 0.3)' : 'none'
                          }}
                        />
                      </div>

                      <button
                        onClick={handleExecuteQuery}
                        onMouseEnter={() => setHoveredBox('fetch-btn')}
                        onMouseLeave={() => setHoveredBox(null)}
                        className="lift-on-hover w-full bg-[#2E7D32] text-white py-4 rounded-lg transition-all duration-300 border-2 cursor-pointer text-xl font-bold mt-4"
                        style={{
                          borderColor: hoveredBox === 'fetch-btn' ? '#F4A300' : '#2E7D32',
                          boxShadow: hoveredBox === 'fetch-btn' ? '0 10px 25px rgba(244, 163, 0, 0.4)' : 'none'
                        }}
                      >
                        Fetch Data
                      </button>
                    </div>
                  </div>
                </div>

                {/* Results Section */}
                <div className="lg:col-span-3 space-y-10">
                  {showQueryResults ? (
                    <>
                      {/* 1. Table Box */}
                      <div 
                        onMouseEnter={() => setHoveredBox('table-results')}
                        onMouseLeave={() => setHoveredBox(null)}
                        className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl p-8 transition-all duration-300 shadow-lg"
                        style={{
                          borderColor: hoveredBox === 'table-results' ? '#2E5BBA' : '#D1D5DB',
                          boxShadow: hoveredBox === 'table-results' ? '0 15px 40px rgba(46, 91, 186, 0.2)' : 'none'
                        }}
                      >
                        <div className="flex justify-between items-center mb-8 pb-4 border-b-2 border-[#E5E7EB]">
                          <h3 className="text-2xl text-[#1C1C1C] m-0 font-bold" style={{ fontFamily: 'var(--font-head)' }}>Data Table</h3>
                          <div className="flex gap-3">
                            <button className="lift-on-hover flex items-center gap-2 px-5 py-2 bg-[#2E7D32] text-white rounded-lg border-2 border-[#2E7D32] hover:border-[#F4A300] cursor-pointer text-base font-bold shadow-md">
                              <Download className="w-5 h-5" />
                              CSV
                            </button>
                            <button className="lift-on-hover flex items-center gap-2 px-5 py-2 bg-[#2E7D32] text-white rounded-lg border-2 border-[#2E7D32] hover:border-[#F4A300] cursor-pointer text-base font-bold shadow-md">
                              <Download className="w-5 h-5" />
                              PDF
                            </button>
                            <button className="lift-on-hover flex items-center gap-2 px-5 py-2 bg-[#2E7D32] text-white rounded-lg border-2 border-[#2E7D32] hover:border-[#F4A300] cursor-pointer text-base font-bold shadow-md">
                              <Download className="w-5 h-5" />
                              JSON
                            </button>
                          </div>
                        </div>

                        <div className="overflow-x-auto border-2 border-[#D1D5DB] rounded-xl bg-white/50">
                          <table className="w-full text-lg">
                            <thead className="bg-[#1F3A5F] text-white">
                              <tr>
                                <th className="px-6 py-4 text-left font-bold uppercase tracking-wider">State</th>
                                <th className="px-6 py-4 text-left font-bold uppercase tracking-wider">Population</th>
                                <th className="px-6 py-4 text-left font-bold uppercase tracking-wider">Literacy</th>
                              </tr>
                            </thead>
                            <tbody>
                              {mockData.map((row) => (
                                <tr key={row.id} className="border-t border-[#E5E7EB] hover:bg-[#2E5BBA]/5 transition-colors">
                                  <td className="px-6 py-4 font-medium text-[#1C1C1C]">{row.state}</td>
                                  <td className="px-6 py-4 text-[#1C1C1C]">{row.population.toLocaleString()}</td>
                                  <td className="px-6 py-4 text-[#6B7280]">{row.literacy}%</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>

                      {/* 2. Visualization Box */}
                      <div 
                        onMouseEnter={() => setHoveredBox('viz-results')}
                        onMouseLeave={() => setHoveredBox(null)}
                        className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl p-8 transition-all duration-300 shadow-lg"
                        style={{
                          borderColor: hoveredBox === 'viz-results' ? '#F4A300' : '#D1D5DB',
                          boxShadow: hoveredBox === 'viz-results' ? '0 15px 40px rgba(244, 163, 0, 0.2)' : 'none'
                        }}
                      >
                        <div className="flex justify-between items-center mb-8 pb-4 border-b-2 border-[#E5E7EB]">
                          <div className="flex items-center gap-6">
                            <h3 className="text-2xl text-[#1C1C1C] m-0 font-bold" style={{ fontFamily: 'var(--font-head)' }}>Visualization</h3>
                            <div className="flex bg-[#F5F7FA] p-1 rounded-lg border-2 border-[#D1D5DB]">
                              <button
                                onClick={() => setChartView("bar")}
                                className={`px-6 py-2 rounded-md font-bold text-lg transition-all ${
                                  chartView === "bar" ? "bg-[#2E5BBA] text-white shadow-md" : "text-[#6B7280] hover:text-[#2E5BBA]"
                                }`}
                              >
                                Bar Chart
                              </button>
                              <button
                                onClick={() => setChartView("pie")}
                                className={`px-6 py-2 rounded-md font-bold text-lg transition-all ${
                                  chartView === "pie" ? "bg-[#2E5BBA] text-white shadow-md" : "text-[#6B7280] hover:text-[#2E5BBA]"
                                }`}
                              >
                                Pie Chart
                              </button>
                            </div>
                          </div>
                          <div className="flex gap-3">
                            <button className="lift-on-hover flex items-center gap-2 px-5 py-2 bg-[#F4A300] text-white rounded-lg border-2 border-[#F4A300] hover:border-[#2E5BBA] cursor-pointer text-base font-bold shadow-md">
                              <Download className="w-5 h-5" />
                              Save PNG
                            </button>
                            <button className="lift-on-hover flex items-center gap-2 px-5 py-2 bg-[#F4A300] text-white rounded-lg border-2 border-[#F4A300] hover:border-[#2E5BBA] cursor-pointer text-base font-bold shadow-md">
                              <Download className="w-5 h-5" />
                              Save PDF
                            </button>
                          </div>
                        </div>

                        <div className="p-6 border-2 border-[#D1D5DB] rounded-xl bg-white/50 min-h-[500px] flex items-center justify-center">
                          {chartView === "bar" ? (
                            <ResponsiveContainer width="100%" height={450}>
                              <BarChart data={chartData} margin={{ top: 20, right: 30, left: 40, bottom: 20 }}>
                                <CartesianGrid strokeDasharray="3 3" />
                                <XAxis dataKey="name" tick={{ fontSize: 14, fontWeight: 600 }} />
                                <YAxis tick={{ fontSize: 14, fontWeight: 600 }} />
                                <Tooltip contentStyle={{ fontSize: '16px', borderRadius: '8px', border: '2px solid #D1D5DB' }} />
                                <Legend wrapperStyle={{ paddingTop: '20px', fontSize: '16px' }} />
                                <Bar dataKey="value" fill="#2E5BBA" name="Population (millions)" radius={[4, 4, 0, 0]} />
                              </BarChart>
                            </ResponsiveContainer>
                          ) : (
                            <ResponsiveContainer width="100%" height={450}>
                              <PieChart>
                                <Pie
                                  data={chartData}
                                  cx="50%"
                                  cy="50%"
                                  labelLine={true}
                                  label={({ name, percent }) => `${name} (${(percent * 100).toFixed(0)}%)`}
                                  outerRadius={160}
                                  fill="#8884d8"
                                  dataKey="value"
                                >
                                  {chartData.map((entry, index) => (
                                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                                  ))}
                                </Pie>
                                <Tooltip contentStyle={{ fontSize: '16px', borderRadius: '8px', border: '2px solid #D1D5DB' }} />
                                <Legend wrapperStyle={{ paddingTop: '20px', fontSize: '16px' }} />
                              </PieChart>
                            </ResponsiveContainer>
                          )}
                        </div>
                      </div>
                    </>
                  ) : (
                    <div 
                      onMouseEnter={() => setHoveredBox('no-results')}
                      onMouseLeave={() => setHoveredBox(null)}
                      className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 border-dashed rounded-xl p-32 text-center transition-all duration-300"
                      style={{
                        borderColor: hoveredBox === 'no-results' ? '#2E5BBA' : '#D1D5DB',
                        boxShadow: hoveredBox === 'no-results' ? '0 20px 60px rgba(46, 91, 186, 0.15)' : 'none'
                      }}
                    >
                      <Search className="w-24 h-24 text-[#D1D5DB] mx-auto mb-6" />
                      <h3 className="text-3xl text-[#1C1C1C] mb-4 font-bold" style={{ fontFamily: 'var(--font-head)' }}>Ready to Query</h3>
                      <p className="text-xl text-[#6B7280]">Choose a schema and table to fetch data results</p>
                    </div>
                  )}
                </div>
              </div>
            </div>
          )}

          {/* Settings */}
          {activeView === "settings" && (
            <div className="max-w-[1200px] mx-auto">
              <h1 className="text-4xl text-[#1C1C1C] mb-10" style={{ fontFamily: 'var(--font-head)', fontWeight: 800 }}>Settings</h1>
              <div 
                onMouseEnter={() => setHoveredBox('settings')}
                onMouseLeave={() => setHoveredBox(null)}
                className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl p-10 space-y-6 transition-all duration-300 shadow-lg"
                style={{
                  borderColor: hoveredBox === 'settings' ? '#F4A300' : '#D1D5DB',
                  boxShadow: hoveredBox === 'settings' ? '0 15px 40px rgba(244, 163, 0, 0.2)' : 'none'
                }}
              >
                <button 
                  onMouseEnter={() => setHoveredBox('change-password')}
                  onMouseLeave={() => setHoveredBox(null)}
                  className="lift-on-hover w-full px-8 py-5 bg-[#F5F7FA] text-[#1C1C1C] rounded-lg border-2 cursor-pointer transition-all duration-300 text-left text-xl font-medium"
                  style={{
                    borderColor: hoveredBox === 'change-password' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredBox === 'change-password' ? '0 10px 20px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                >
                  Change Password
                </button>
                <button 
                  onMouseEnter={() => setHoveredBox('change-username')}
                  onMouseLeave={() => setHoveredBox(null)}
                  className="lift-on-hover w-full px-8 py-5 bg-[#F5F7FA] text-[#1C1C1C] rounded-lg border-2 cursor-pointer transition-all duration-300 text-left text-xl font-medium"
                  style={{
                    borderColor: hoveredBox === 'change-username' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredBox === 'change-username' ? '0 10px 20px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                >
                  Change Username
                </button>
                <button 
                  onMouseEnter={() => setHoveredBox('delete-account')}
                  onMouseLeave={() => setHoveredBox(null)}
                  className="lift-on-hover w-full px-8 py-5 bg-[#F5F7FA] text-[#d4183d] rounded-lg border-2 cursor-pointer transition-all duration-300 text-left text-xl font-medium"
                  style={{
                    borderColor: hoveredBox === 'delete-account' ? '#d4183d' : '#D1D5DB',
                    boxShadow: hoveredBox === 'delete-account' ? '0 10px 20px rgba(212, 24, 61, 0.2)' : 'none'
                  }}
                >
                  Delete Account
                </button>
              </div>
            </div>
          )}

          {/* Plans & Pricing */}
          {activeView === "plans" && (
            <div className="max-w-[1400px] mx-auto text-center">
              <h1 className="text-6xl text-[#1C1C1C] mb-4 font-black" style={{ fontFamily: 'var(--font-head)' }}>Plans & Pricing</h1>
              <p className="text-2xl text-[#6B7280] mb-16 max-w-3xl mx-auto leading-relaxed font-medium">
                Choose the perfect plan for your needs. All plans include access to our AI-powered analytics platform with verified government data.
              </p>
              
              <div className="grid grid-cols-1 md:grid-cols-3 gap-12 items-start">
                {/* Free Plan */}
                <div 
                  onMouseEnter={() => setHoveredBox('plan-free')}
                  onMouseLeave={() => setHoveredBox(null)}
                  className="lift-on-hover bg-white border-2 rounded-3xl relative overflow-hidden transition-all duration-300 flex flex-col h-full shadow-xl"
                  style={{ 
                    borderColor: hoveredBox === 'plan-free' ? '#2E7D32' : '#E5E7EB',
                    boxShadow: hoveredBox === 'plan-free' ? '0 25px 60px rgba(46, 125, 50, 0.2)' : '0 10px 30px rgba(0,0,0,0.05)'
                  }}
                >
                  <div className="bg-[#2E7D32] text-white py-3 text-xl font-bold">Current Plan</div>
                  <div className="p-12 flex flex-col h-full items-start text-left">
                    <div className="w-16 h-16 bg-[#F5F7FA] rounded-2xl flex items-center justify-center mb-8 border-2 border-[#E5E7EB]">
                      <Shield className="w-8 h-8 text-[#2E5BBA]" />
                    </div>
                    <h2 className="text-4xl text-[#1C1C1C] mb-3 font-black" style={{ fontFamily: 'var(--font-head)' }}>Free</h2>
                    <p className="text-xl text-[#6B7280] mb-8 font-medium">Perfect for getting started</p>
                    <div className="flex items-baseline gap-2 mb-10">
                      <span className="text-6xl font-black text-[#1C1C1C]">₹0</span>
                      <span className="text-2xl text-[#6B7280] font-bold">/month</span>
                    </div>
                    <button className="w-full bg-[#E5E7EB] text-[#6B7280] py-5 rounded-2xl text-2xl font-black mb-12 cursor-not-allowed">
                      Current Plan
                    </button>
                    <div className="space-y-6 w-full">
                      <p className="text-[#6B7280] text-lg font-black uppercase tracking-widest">What's included:</p>
                      <div className="flex items-center gap-4 text-xl font-bold text-[#1C1C1C]">
                        <Check className="w-6 h-6 text-[#2E7D32]" />
                        <span>10 queries per month</span>
                      </div>
                    </div>
                  </div>
                </div>

                {/* Basic Plan */}
                <div 
                  onMouseEnter={() => setHoveredBox('plan-basic')}
                  onMouseLeave={() => setHoveredBox(null)}
                  className="lift-on-hover bg-white border-2 rounded-3xl relative overflow-hidden transition-all duration-300 flex flex-col h-full shadow-xl"
                  style={{ 
                    borderColor: hoveredBox === 'plan-basic' ? '#2E5BBA' : '#E5E7EB',
                    boxShadow: hoveredBox === 'plan-basic' ? '0 25px 60px rgba(46, 91, 186, 0.2)' : '0 10px 30px rgba(0,0,0,0.05)'
                  }}
                >
                  <div className="p-12 flex flex-col h-full items-start text-left pt-20">
                    <div className="w-16 h-16 bg-[#F5F7FA] rounded-2xl flex items-center justify-center mb-8 border-2 border-[#E5E7EB]">
                      <Zap className="w-8 h-8 text-[#2E5BBA]" />
                    </div>
                    <h2 className="text-4xl text-[#1C1C1C] mb-3 font-black" style={{ fontFamily: 'var(--font-head)' }}>Basic</h2>
                    <p className="text-xl text-[#6B7280] mb-8 font-medium">For regular users</p>
                    <div className="flex items-baseline gap-2 mb-10">
                      <span className="text-6xl font-black text-[#1C1C1C]">₹999</span>
                      <span className="text-2xl text-[#6B7280] font-bold">/month</span>
                    </div>
                    <button className="lift-on-hover w-full bg-white text-[#2E5BBA] py-5 rounded-2xl text-2xl font-black mb-12 border-2 border-[#2E5BBA] hover:bg-[#2E5BBA] hover:text-white transition-all shadow-lg">
                      Upgrade to Basic
                    </button>
                    <div className="space-y-6 w-full">
                      <p className="text-[#6B7280] text-lg font-black uppercase tracking-widest">What's included:</p>
                      <div className="flex items-center gap-4 text-xl font-bold text-[#1C1C1C]">
                        <Check className="w-6 h-6 text-[#2E7D32]" />
                        <span>100 queries per month</span>
                      </div>
                      <div className="flex items-center gap-4 text-xl font-bold text-[#1C1C1C]">
                        <Check className="w-6 h-6 text-[#2E7D32]" />
                        <span>Advanced analytics</span>
                      </div>
                    </div>
                  </div>
                </div>

                {/* Premium Plan */}
                <div 
                  onMouseEnter={() => setHoveredBox('plan-premium')}
                  onMouseLeave={() => setHoveredBox(null)}
                  className="lift-on-hover bg-white border-2 rounded-3xl relative overflow-hidden transition-all duration-300 flex flex-col h-full shadow-2xl scale-[1.02]"
                  style={{ 
                    borderColor: hoveredBox === 'plan-premium' ? '#2E5BBA' : '#2E5BBA',
                    boxShadow: hoveredBox === 'plan-premium' ? '0 30px 80px rgba(46, 91, 186, 0.3)' : '0 15px 50px rgba(46, 91, 186, 0.15)'
                  }}
                >
                  <div className="bg-[#2E5BBA] text-white py-3 text-xl font-bold uppercase tracking-widest">Most Popular</div>
                  <div className="p-12 flex flex-col h-full items-start text-left">
                    <div className="w-16 h-16 bg-[#F5F7FA] rounded-2xl flex items-center justify-center mb-8 border-2 border-[#E5E7EB]">
                      <Crown className="w-8 h-8 text-[#2E5BBA]" />
                    </div>
                    <h2 className="text-4xl text-[#1C1C1C] mb-3 font-black" style={{ fontFamily: 'var(--font-head)' }}>Premium</h2>
                    <p className="text-xl text-[#6B7280] mb-8 font-medium">For power users and teams</p>
                    <div className="flex items-baseline gap-2 mb-10">
                      <span className="text-6xl font-black text-[#1C1C1C]">₹2,999</span>
                      <span className="text-2xl text-[#6B7280] font-bold">/month</span>
                    </div>
                    <button className="lift-on-hover w-full bg-[#2E5BBA] text-white py-5 rounded-2xl text-2xl font-black mb-12 shadow-xl hover:bg-[#1F3A5F] transition-all">
                      Upgrade to Premium
                    </button>
                    <div className="space-y-6 w-full">
                      <p className="text-[#6B7280] text-lg font-black uppercase tracking-widest">What's included:</p>
                      <div className="flex items-center gap-4 text-xl font-bold text-[#1C1C1C]">
                        <Check className="w-6 h-6 text-[#2E7D32]" />
                        <span>Unlimited queries</span>
                      </div>
                      <div className="flex items-center gap-4 text-xl font-bold text-[#1C1C1C]">
                        <Check className="w-6 h-6 text-[#2E7D32]" />
                        <span>Real-time analytics</span>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* History */}
          {activeView === "history" && (
            <div className="max-w-[1600px] mx-auto">
              <h1 className="text-4xl text-[#1C1C1C] mb-10" style={{ fontFamily: 'var(--font-head)', fontWeight: 800 }}>Query History</h1>
              <div 
                onMouseEnter={() => setHoveredBox('history')}
                onMouseLeave={() => setHoveredBox(null)}
                className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl overflow-hidden transition-all duration-300 shadow-lg"
                style={{
                  borderColor: hoveredBox === 'history' ? '#2E5BBA' : '#D1D5DB',
                  boxShadow: hoveredBox === 'history' ? '0 15px 40px rgba(46, 91, 186, 0.2)' : 'none'
                }}
              >
                <table className="w-full">
                  <thead className="bg-[#1F3A5F] text-white">
                    <tr>
                      <th className="px-8 py-5 text-left text-xl font-bold uppercase tracking-wider">Query</th>
                      <th className="px-8 py-5 text-left text-xl font-bold uppercase tracking-wider">Date</th>
                      <th className="px-8 py-5 text-left text-xl font-bold uppercase tracking-wider">Results</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr className="border-t border-[#E5E7EB] hover:bg-[#2E5BBA]/5 transition-colors">
                      <td className="px-8 py-6 text-xl font-medium text-[#1C1C1C]">Census Data - States</td>
                      <td className="px-8 py-6 text-xl text-[#6B7280]">2026-03-18</td>
                      <td className="px-8 py-6 text-xl text-[#1C1C1C]">28 rows</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Downloads */}
          {activeView === "downloads" && (
            <div className="max-w-[1600px] mx-auto">
              <h1 className="text-4xl text-[#1C1C1C] mb-10" style={{ fontFamily: 'var(--font-head)', fontWeight: 800 }}>Downloads</h1>
              <div 
                onMouseEnter={() => setHoveredBox('downloads')}
                onMouseLeave={() => setHoveredBox(null)}
                className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl overflow-hidden transition-all duration-300 shadow-lg"
                style={{
                  borderColor: hoveredBox === 'downloads' ? '#2E7D32' : '#D1D5DB',
                  boxShadow: hoveredBox === 'downloads' ? '0 15px 40px rgba(46, 125, 50, 0.2)' : 'none'
                }}
              >
                <table className="w-full">
                  <thead className="bg-[#1b5e20] text-white">
                    <tr>
                      <th className="px-8 py-5 text-left text-xl font-bold uppercase tracking-wider">File Name</th>
                      <th className="px-8 py-5 text-left text-xl font-bold uppercase tracking-wider">Date</th>
                      <th className="px-8 py-5 text-left text-xl font-bold uppercase tracking-wider">Action</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr className="border-t border-[#E5E7EB] hover:bg-[#2E7D32]/5 transition-colors">
                      <td className="px-8 py-6 text-xl font-medium text-[#1C1C1C]">census_states_2021.csv</td>
                      <td className="px-8 py-6 text-xl text-[#6B7280]">2026-03-18</td>
                      <td className="px-8 py-6">
                        <button className="lift-on-hover flex items-center gap-3 px-6 py-3 bg-[#2E7D32] text-white rounded-lg border-2 border-[#2E7D32] hover:border-[#F4A300] cursor-pointer text-lg font-bold shadow-md">
                          <Download className="w-6 h-6" />
                          Download
                        </button>
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}