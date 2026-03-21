import { useState } from "react";
import { useNavigate } from "react-router";
import { Header } from "../../components/Header";
import { Navigation } from "../../components/Navigation";
import { BackButton } from "../../components/BackButton";
import { AnimatedBackground } from "../../components/AnimatedBackground";

export function ConfigureVariables() {
  const navigate = useNavigate();
  const adminData = JSON.parse(sessionStorage.getItem("adminData") || '{"fullName":"Admin"}');
  const [formData, setFormData] = useState({
    schema: "",
    table: "",
    variableName: "",
    filters: "",
  });
  const [hoveredBox, setHoveredBox] = useState<string | null>(null);

  return (
    <div className="min-h-screen relative">
      <AnimatedBackground />
      
      <div className="relative z-10">
        <Header userName={adminData.fullName} userRole="admin" />
        <Navigation userRole="admin" />

        <div className="max-w-[1400px] mx-auto px-10 py-12">
          <div className="mb-10 flex justify-between items-center">
            <BackButton to="/admin/dashboard" />
            <h1 className="text-4xl text-[#1C1C1C] m-0" style={{ fontFamily: 'var(--font-head)', fontWeight: 800 }}>Configure Variables</h1>
            <div className="w-[80px]"></div>
          </div>

          <div 
            onMouseEnter={() => setHoveredBox('main')}
            onMouseLeave={() => setHoveredBox(null)}
            className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl p-12 transition-all duration-300 shadow-xl"
            style={{
              borderColor: hoveredBox === 'main' || hoveredBox?.includes('-') ? '#2E5BBA' : '#D1D5DB',
              boxShadow: hoveredBox === 'main' || hoveredBox?.includes('-') ? '0 15px 40px rgba(46, 91, 186, 0.2)' : 'none'
            }}
          >
            <div className="space-y-10">
              <div>
                <label className="block text-[#1C1C1C] mb-4 font-bold text-xl">Select Schema</label>
                <select
                  value={formData.schema}
                  onChange={(e) => setFormData({ ...formData, schema: e.target.value })}
                  onMouseEnter={() => setHoveredBox('schema-select')}
                  onMouseLeave={() => setHoveredBox('main')}
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                  style={{
                    borderColor: hoveredBox === 'schema-select' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredBox === 'schema-select' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                >
                  <option value="">Choose schema...</option>
                  <option value="census">Census</option>
                  <option value="economic">Economic</option>
                </select>
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-4 font-bold text-xl">Select Table</label>
                <select
                  value={formData.table}
                  onChange={(e) => setFormData({ ...formData, table: e.target.value })}
                  onMouseEnter={() => setHoveredBox('table-select')}
                  onMouseLeave={() => setHoveredBox('main')}
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                  style={{
                    borderColor: hoveredBox === 'table-select' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredBox === 'table-select' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                >
                  <option value="">Choose table...</option>
                  <option value="states">States</option>
                  <option value="districts">Districts</option>
                </select>
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-4 font-bold text-xl">Variable Name</label>
                <input
                  type="text"
                  value={formData.variableName}
                  onChange={(e) => setFormData({ ...formData, variableName: e.target.value })}
                  onMouseEnter={() => setHoveredBox('variable-name')}
                  onMouseLeave={() => setHoveredBox('main')}
                  placeholder="Enter variable name"
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                  style={{
                    borderColor: hoveredBox === 'variable-name' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredBox === 'variable-name' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                />
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-4 font-bold text-xl">Filters</label>
                <input
                  type="text"
                  value={formData.filters}
                  onChange={(e) => setFormData({ ...formData, filters: e.target.value })}
                  onMouseEnter={() => setHoveredBox('filters-input')}
                  onMouseLeave={() => setHoveredBox('main')}
                  placeholder="Enter filter conditions"
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                  style={{
                    borderColor: hoveredBox === 'filters-input' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredBox === 'filters-input' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                />
              </div>

              <button 
                onMouseEnter={() => setHoveredBox('save-btn')}
                onMouseLeave={() => setHoveredBox('main')}
                className="lift-on-hover w-full bg-[#2E5BBA] text-white py-5 rounded-xl border-2 cursor-pointer transition-all duration-300 text-2xl font-black shadow-lg mt-6"
                style={{
                  borderColor: hoveredBox === 'save-btn' ? '#F4A300' : '#2E5BBA',
                  boxShadow: hoveredBox === 'save-btn' ? '0 10px 25px rgba(244, 163, 0, 0.4)' : 'none'
                }}
              >
                Save Configuration
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}