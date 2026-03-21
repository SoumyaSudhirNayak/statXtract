import { useState } from "react";
import { useNavigate } from "react-router";
import { Header } from "../../components/Header";
import { Navigation } from "../../components/Navigation";
import { BackButton } from "../../components/BackButton";
import { AnimatedBackground } from "../../components/AnimatedBackground";

export function Metadata() {
  const navigate = useNavigate();
  const adminData = JSON.parse(sessionStorage.getItem("adminData") || '{"fullName":"Admin"}');
  const [formData, setFormData] = useState({ schema: "", table: "" });
  const [showMetadata, setShowMetadata] = useState(false);
  const [hoveredBox, setHoveredBox] = useState<string | null>(null);

  const metadata = {
    description: "Census 2021 state-wise population data",
    created: "2021-05-15",
    updated: "2026-01-10",
    records: 28,
    source: "Census of India",
  };

  const handleView = () => {
    if (formData.schema && formData.table) {
      setShowMetadata(true);
    }
  };

  return (
    <div className="min-h-screen relative">
      <AnimatedBackground />
      
      <div className="relative z-10">
        <Header userName={adminData.fullName} userRole="admin" />
        <Navigation userRole="admin" />

        <div className="max-w-[1600px] mx-auto px-10 py-12">
          <div className="mb-10 flex justify-between items-center">
            <BackButton to="/admin/dashboard" />
            <h1 className="text-4xl text-[#1C1C1C] m-0" style={{ fontFamily: 'var(--font-head)', fontWeight: 800 }}>Metadata</h1>
            <div className="w-[80px]"></div>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-12">
            {/* Selection Form */}
            <div 
              onMouseEnter={() => setHoveredBox('selection')}
              onMouseLeave={() => setHoveredBox(null)}
              className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl p-10 transition-all duration-300 shadow-xl"
              style={{
                borderColor: hoveredBox === 'selection' ? '#2E5BBA' : '#D1D5DB',
                boxShadow: hoveredBox === 'selection' ? '0 15px 40px rgba(46, 91, 186, 0.2)' : 'none'
              }}
            >
              <h3 className="text-3xl text-[#1C1C1C] mb-8 font-bold" style={{ fontFamily: 'var(--font-head)' }}>Select Dataset</h3>
              <div className="space-y-8">
                <div>
                  <label className="block text-[#1C1C1C] mb-3 text-lg font-bold">Select Schema</label>
                  <select
                    value={formData.schema}
                    onChange={(e) => setFormData({ ...formData, schema: e.target.value })}
                    onMouseEnter={() => setHoveredBox('schema-select')}
                    onMouseLeave={() => setHoveredBox('selection')}
                    className="w-full px-5 py-3 border-2 rounded-lg focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl"
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
                  <label className="block text-[#1C1C1C] mb-3 text-lg font-bold">Select Table</label>
                  <select
                    value={formData.table}
                    onChange={(e) => setFormData({ ...formData, table: e.target.value })}
                    onMouseEnter={() => setHoveredBox('table-select')}
                    onMouseLeave={() => setHoveredBox('selection')}
                    className="w-full px-5 py-3 border-2 rounded-lg focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl"
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

                <button
                  onClick={handleView}
                  disabled={!formData.schema || !formData.table}
                  onMouseEnter={() => setHoveredBox('view-btn')}
                  onMouseLeave={() => setHoveredBox('selection')}
                  className="lift-on-hover w-full bg-[#2E5BBA] text-white py-4 rounded-xl transition-all duration-300 border-2 cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed text-xl font-bold shadow-lg"
                  style={{
                    borderColor: hoveredBox === 'view-btn' ? '#F4A300' : '#2E5BBA',
                    boxShadow: hoveredBox === 'view-btn' ? '0 10px 25px rgba(244, 163, 0, 0.4)' : 'none'
                  }}
                >
                  View Metadata
                </button>
              </div>
            </div>

            {/* Metadata Display */}
            <div 
              onMouseEnter={() => setHoveredBox('display')}
              onMouseLeave={() => setHoveredBox(null)}
              className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl p-10 transition-all duration-300 shadow-xl"
              style={{
                borderColor: hoveredBox === 'display' ? '#2E7D32' : '#D1D5DB',
                boxShadow: hoveredBox === 'display' ? '0 15px 40px rgba(46, 125, 50, 0.2)' : 'none'
              }}
            >
              {showMetadata ? (
                <div>
                  <h3 className="text-3xl text-[#1C1C1C] mb-8 font-bold" style={{ fontFamily: 'var(--font-head)' }}>Metadata Details</h3>
                  <div className="space-y-8">
                    <div className="pb-6 border-b-2 border-[#E5E7EB]">
                      <p className="text-[#6B7280] text-sm mb-3 font-black uppercase tracking-widest">Description</p>
                      <p className="text-[#1C1C1C] m-0 text-2xl leading-relaxed font-medium">{metadata.description}</p>
                    </div>
                    <div className="grid grid-cols-2 gap-8">
                      <div className="bg-[#F5F7FA] p-6 rounded-xl border-2 border-[#D1D5DB]">
                        <p className="text-[#6B7280] text-xs mb-2 font-black uppercase tracking-wider">Created</p>
                        <p className="text-[#1C1C1C] m-0 text-xl font-bold">{metadata.created}</p>
                      </div>
                      <div className="bg-[#F5F7FA] p-6 rounded-xl border-2 border-[#D1D5DB]">
                        <p className="text-[#6B7280] text-xs mb-2 font-black uppercase tracking-wider">Last Updated</p>
                        <p className="text-[#1C1C1C] m-0 text-xl font-bold">{metadata.updated}</p>
                      </div>
                      <div className="bg-[#F5F7FA] p-6 rounded-xl border-2 border-[#D1D5DB]">
                        <p className="text-[#6B7280] text-xs mb-2 font-black uppercase tracking-wider">Total Records</p>
                        <p className="text-[#1C1C1C] m-0 text-xl font-bold">{metadata.records.toLocaleString()}</p>
                      </div>
                      <div className="bg-[#F5F7FA] p-6 rounded-xl border-2 border-[#D1D5DB]">
                        <p className="text-[#6B7280] text-xs mb-2 font-black uppercase tracking-wider">Source</p>
                        <p className="text-[#1C1C1C] m-0 text-xl font-bold">{metadata.source}</p>
                      </div>
                    </div>
                  </div>
                </div>
              ) : (
                <div className="h-full flex flex-col items-center justify-center text-center p-12">
                  <div className="w-32 h-32 bg-[#F5F7FA] rounded-full flex items-center justify-center mb-6 border-2 border-dashed border-[#D1D5DB]">
                    <p className="text-6xl m-0">📊</p>
                  </div>
                  <p className="text-[#6B7280] text-2xl font-bold leading-relaxed">Select a dataset to view detailed metadata information</p>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}