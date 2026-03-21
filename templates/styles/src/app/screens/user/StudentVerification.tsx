import { useState, useRef } from "react";
import { useNavigate } from "react-router";
import { BackButton } from "../../components/BackButton";
import { AnimatedBackground } from "../../components/AnimatedBackground";

export function StudentVerification() {
  const navigate = useNavigate();
  const [formData, setFormData] = useState({
    institutionName: "",
    studentIdName: "",
    studentIdNumber: "",
    courseName: "",
    yearOfStudy: "",
    institutionEmail: "",
  });
  const [file, setFile] = useState<File | null>(null);
  const [hoveredInput, setHoveredInput] = useState<string | null>(null);
  const [isContainerHovered, setIsContainerHovered] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const userData = JSON.parse(sessionStorage.getItem("userData") || "{}");
    sessionStorage.setItem("userData", JSON.stringify({ ...userData, ...formData, orgType: "Student" }));
    navigate("/user/dashboard");
  };

  return (
    <div className="min-h-screen relative">
      <AnimatedBackground />
      
      <div className="relative z-10">
        <div className="bg-[#1F3A5F] text-white px-6 py-4 shadow-lg">
          <h1 className="text-white m-0 max-w-[1200px] mx-auto" style={{ fontFamily: 'var(--font-head)', fontWeight: 700 }}>
            SPATXTRACT
          </h1>
        </div>

        <div className="max-w-[1000px] mx-auto px-6 py-20">
          <div className="mb-10">
            <BackButton to="/user/organization-type" />
          </div>
          
          <h2 className="text-5xl text-[#1C1C1C] mb-12 text-center font-black" style={{ fontFamily: 'var(--font-head)' }}>
            Student Verification
          </h2>

          <form 
            ref={containerRef}
            onSubmit={handleSubmit} 
            onMouseEnter={() => setIsContainerHovered(true)}
            onMouseLeave={() => setIsContainerHovered(false)}
            className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-2xl p-12 relative overflow-hidden shadow-2xl"
            style={{
              borderColor: isContainerHovered ? '#D32F2F' : '#D1D5DB',
              boxShadow: isContainerHovered 
                ? '0 30px 70px rgba(211, 47, 47, 0.25)'
                : 'none',
            }}
          >
            <div className="space-y-8 relative z-10">
              <div>
                <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Institution Name</label>
                <input
                  type="text"
                  value={formData.institutionName}
                  onChange={(e) => setFormData({ ...formData, institutionName: e.target.value })}
                  onMouseEnter={() => setHoveredInput('institutionName')}
                  onMouseLeave={() => setHoveredInput(null)}
                  required
                  placeholder="University or College Name"
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner"
                  style={{
                    borderColor: hoveredInput === 'institutionName' ? '#D32F2F' : '#D1D5DB',
                    boxShadow: hoveredInput === 'institutionName' ? '0 0 15px rgba(211, 47, 47, 0.2)' : 'none'
                  }}
                />
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                <div>
                  <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Student Name (on ID)</label>
                  <input
                    type="text"
                    value={formData.studentIdName}
                    onChange={(e) => setFormData({ ...formData, studentIdName: e.target.value })}
                    onMouseEnter={() => setHoveredInput('studentIdName')}
                    onMouseLeave={() => setHoveredInput(null)}
                    required
                    className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner"
                    style={{
                      borderColor: hoveredInput === 'studentIdName' ? '#1976D2' : '#D1D5DB',
                      boxShadow: hoveredInput === 'studentIdName' ? '0 0 15px rgba(25, 118, 210, 0.2)' : 'none'
                    }}
                  />
                </div>
                <div>
                  <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Student ID Number</label>
                  <input
                    type="text"
                    value={formData.studentIdNumber}
                    onChange={(e) => setFormData({ ...formData, studentIdNumber: e.target.value })}
                    onMouseEnter={() => setHoveredInput('studentIdNumber')}
                    onMouseLeave={() => setHoveredInput(null)}
                    required
                    className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner"
                    style={{
                      borderColor: hoveredInput === 'studentIdNumber' ? '#2E7D32' : '#D1D5DB',
                      boxShadow: hoveredInput === 'studentIdNumber' ? '0 0 15px rgba(46, 125, 50, 0.2)' : 'none'
                    }}
                  />
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                <div>
                  <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Course Name</label>
                  <input
                    type="text"
                    value={formData.courseName}
                    onChange={(e) => setFormData({ ...formData, courseName: e.target.value })}
                    onMouseEnter={() => setHoveredInput('courseName')}
                    onMouseLeave={() => setHoveredInput(null)}
                    required
                    className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner"
                    style={{
                      borderColor: hoveredInput === 'courseName' ? '#7B1FA2' : '#D1D5DB',
                      boxShadow: hoveredInput === 'courseName' ? '0 0 15px rgba(123, 31, 162, 0.2)' : 'none'
                    }}
                  />
                </div>
                <div>
                  <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Year of Study</label>
                  <input
                    type="text"
                    value={formData.yearOfStudy}
                    onChange={(e) => setFormData({ ...formData, yearOfStudy: e.target.value })}
                    onMouseEnter={() => setHoveredInput('yearOfStudy')}
                    onMouseLeave={() => setHoveredInput(null)}
                    required
                    className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner"
                    style={{
                      borderColor: hoveredInput === 'yearOfStudy' ? '#F57C00' : '#D1D5DB',
                      boxShadow: hoveredInput === 'yearOfStudy' ? '0 0 15px rgba(245, 124, 0, 0.2)' : 'none'
                    }}
                  />
                </div>
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Institutional Email</label>
                <input
                  type="email"
                  value={formData.institutionEmail}
                  onChange={(e) => setFormData({ ...formData, institutionEmail: e.target.value })}
                  onMouseEnter={() => setHoveredInput('institutionEmail')}
                  onMouseLeave={() => setHoveredInput(null)}
                  required
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner"
                  style={{
                    borderColor: hoveredInput === 'institutionEmail' ? '#D32F2F' : '#D1D5DB',
                    boxShadow: hoveredInput === 'institutionEmail' ? '0 0 15px rgba(211, 47, 47, 0.2)' : 'none'
                  }}
                />
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Upload Student ID (PDF/Image)</label>
                <input
                  type="file"
                  onChange={(e) => setFile(e.target.files?.[0] || null)}
                  onMouseEnter={() => setHoveredInput('file')}
                  onMouseLeave={() => setHoveredInput(null)}
                  required
                  className="w-full px-6 py-4 border-2 border-dashed rounded-xl focus:outline-none bg-white transition-all duration-300 text-xl cursor-pointer"
                  style={{
                    borderColor: hoveredInput === 'file' ? '#1976D2' : '#D1D5DB',
                    boxShadow: hoveredInput === 'file' ? '0 0 15px rgba(25, 118, 210, 0.2)' : 'none'
                  }}
                />
              </div>

              <button
                type="submit"
                onMouseEnter={() => setHoveredInput('submit')}
                onMouseLeave={() => setHoveredInput(null)}
                className="lift-on-hover w-full bg-[#D32F2F] text-white py-5 rounded-xl border-2 cursor-pointer transition-all duration-300 text-2xl font-black shadow-lg mt-6"
                style={{
                  borderColor: hoveredInput === 'submit' ? '#F4A300' : '#D32F2F',
                  boxShadow: hoveredInput === 'submit' ? '0 15px 40px rgba(244, 163, 0, 0.5)' : 'none'
                }}
              >
                Complete Student Verification
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>
  );
}