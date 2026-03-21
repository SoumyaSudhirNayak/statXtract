import { useState, useRef } from "react";
import { useNavigate } from "react-router";
import { BackButton } from "../../components/BackButton";
import { AnimatedBackground } from "../../components/AnimatedBackground";

export function ResearcherVerification() {
  const navigate = useNavigate();
  const [formData, setFormData] = useState({
    researchArea: "",
    designation: "",
    researchId: "",
    researchEmail: "",
  });
  const [file, setFile] = useState<File | null>(null);
  const [hoveredInput, setHoveredInput] = useState<string | null>(null);
  const [isContainerHovered, setIsContainerHovered] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const userData = JSON.parse(sessionStorage.getItem("userData") || "{}");
    sessionStorage.setItem("userData", JSON.stringify({ ...userData, ...formData, orgType: "Researcher" }));
    navigate("/user/dashboard");
  };

  return (
    <div className="min-h-screen relative">
      <AnimatedBackground />
      
      <div className="relative z-10">
        <div className="bg-[#1F3A5F] text-white px-10 py-6 shadow-xl relative z-50">
          <div className="max-w-[1800px] mx-auto flex justify-between items-center">
            <h1 
              className="text-4xl m-0 font-black tracking-tighter cursor-pointer hover:opacity-80 transition-opacity" 
              style={{ fontFamily: 'var(--font-head)', color: 'white' }}
              onClick={() => navigate("/")}
            >
              STATXTRACT
            </h1>
          </div>
        </div>

        <div className="max-w-[1000px] mx-auto px-6 py-20">
          <div className="mb-10">
            <BackButton to="/user/organization-type" />
          </div>
          
          <h2 className="text-5xl text-[#1C1C1C] mb-12 text-center font-black" style={{ fontFamily: 'var(--font-head)' }}>
            Researcher Verification
          </h2>

          <form 
            ref={containerRef}
            onSubmit={handleSubmit} 
            onMouseEnter={() => setIsContainerHovered(true)}
            onMouseLeave={() => setIsContainerHovered(false)}
            className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-2xl p-12 relative overflow-hidden shadow-2xl"
            style={{
              borderColor: isContainerHovered ? '#1976D2' : '#D1D5DB',
              boxShadow: isContainerHovered 
                ? '0 30px 70px rgba(25, 118, 210, 0.25)'
                : 'none',
            }}
          >
            <div className="space-y-8 relative z-10">
              <div>
                <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Research Area</label>
                <input
                  type="text"
                  value={formData.researchArea}
                  onChange={(e) => setFormData({ ...formData, researchArea: e.target.value })}
                  onMouseEnter={() => setHoveredInput('researchArea')}
                  onMouseLeave={() => setHoveredInput(null)}
                  required
                  placeholder="e.g. Data Science, Economics, etc."
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner font-bold"
                  style={{
                    borderColor: hoveredInput === 'researchArea' ? '#1976D2' : '#D1D5DB',
                    boxShadow: hoveredInput === 'researchArea' ? '0 0 15px rgba(25, 118, 210, 0.2)' : 'none'
                  }}
                />
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Designation</label>
                <input
                  type="text"
                  value={formData.designation}
                  onChange={(e) => setFormData({ ...formData, designation: e.target.value })}
                  onMouseEnter={() => setHoveredInput('designation')}
                  onMouseLeave={() => setHoveredInput(null)}
                  required
                  placeholder="e.g. Senior Researcher, Lead Analyst"
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner font-bold"
                  style={{
                    borderColor: hoveredInput === 'designation' ? '#D32F2F' : '#D1D5DB',
                    boxShadow: hoveredInput === 'designation' ? '0 0 15px rgba(211, 47, 47, 0.2)' : 'none'
                  }}
                />
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Research ID / License Number</label>
                <input
                  type="text"
                  value={formData.researchId}
                  onChange={(e) => setFormData({ ...formData, researchId: e.target.value })}
                  onMouseEnter={() => setHoveredInput('researchId')}
                  onMouseLeave={() => setHoveredInput(null)}
                  required
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner font-bold"
                  style={{
                    borderColor: hoveredInput === 'researchId' ? '#2E7D32' : '#D1D5DB',
                    boxShadow: hoveredInput === 'researchId' ? '0 0 15px rgba(46, 125, 50, 0.2)' : 'none'
                  }}
                />
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Institutional Email</label>
                <input
                  type="email"
                  value={formData.researchEmail}
                  onChange={(e) => setFormData({ ...formData, researchEmail: e.target.value })}
                  onMouseEnter={() => setHoveredInput('researchEmail')}
                  onMouseLeave={() => setHoveredInput(null)}
                  required
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner font-bold"
                  style={{
                    borderColor: hoveredInput === 'researchEmail' ? '#F57C00' : '#D1D5DB',
                    boxShadow: hoveredInput === 'researchEmail' ? '0 0 15px rgba(245, 124, 0, 0.2)' : 'none'
                  }}
                />
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Upload Research Credentials (PDF/Image)</label>
                <input
                  type="file"
                  onChange={(e) => setFile(e.target.files?.[0] || null)}
                  onMouseEnter={() => setHoveredInput('file')}
                  onMouseLeave={() => setHoveredInput(null)}
                  required
                  className="w-full px-6 py-4 border-2 border-dashed rounded-xl focus:outline-none bg-white transition-all duration-300 text-xl cursor-pointer font-bold"
                  style={{
                    borderColor: hoveredInput === 'file' ? '#7B1FA2' : '#D1D5DB',
                    boxShadow: hoveredInput === 'file' ? '0 0 15px rgba(123, 31, 162, 0.2)' : 'none'
                  }}
                />
              </div>

              <button
                type="submit"
                onMouseEnter={() => setHoveredInput('submit')}
                onMouseLeave={() => setHoveredInput(null)}
                className="lift-on-hover w-full bg-[#1976D2] text-white py-5 rounded-xl border-2 cursor-pointer transition-all duration-300 text-2xl font-black shadow-lg mt-6"
                style={{
                  borderColor: hoveredInput === 'submit' ? '#F4A300' : '#1976D2',
                  boxShadow: hoveredInput === 'submit' ? '0 15px 40px rgba(244, 163, 0, 0.5)' : 'none'
                }}
              >
                Complete Researcher Verification
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>
  );
}
